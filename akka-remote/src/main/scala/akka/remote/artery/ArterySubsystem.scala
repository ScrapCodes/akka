/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import java.util.concurrent.ConcurrentHashMap

import java.util.function.{ Function ⇒ JFunction }

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.Done
import akka.actor.{ ActorRef, Address, ExtendedActorSystem }
import akka.actor.RootActorPath
import akka.dispatch.sysmsg.SystemMessage
import akka.event.{ Logging, LoggingAdapter }
import akka.remote.{ RemoteActorRef, RemoteActorRefProvider, RemoteTransport }
import akka.remote.AddressUidExtension
import akka.remote.EndpointManager.Send
import akka.remote.UniqueAddress
import akka.remote.artery.ReplyJunction.ReplySubject
import akka.remote.transport.AkkaPduProtobufCodec
import akka.stream.{ ActorMaterializer, Materializer, OverflowStrategy }
import akka.stream.scaladsl.{ Source, SourceQueueWithComplete, Tcp }

/**
 * INTERNAL API
 */
private[remote] class ArterySubsystem(_system: ExtendedActorSystem, _provider: RemoteActorRefProvider) extends RemoteTransport(_system, _provider) {
  import provider.remoteSettings

  @volatile private[this] var localAddress: UniqueAddress = _
  @volatile private[this] var transport: Transport = _
  @volatile private[this] var tcpBinding: Option[Tcp.ServerBinding] = None
  @volatile private[this] var materializer: Materializer = _
  override val log: LoggingAdapter = Logging(system.eventStream, getClass.getName)

  override def defaultAddress: Address = localAddress.address
  override def addresses: Set[Address] = Set(defaultAddress)
  override def localAddressForRemote(remote: Address): Address = defaultAddress

  // FIXME: This does locking on putIfAbsent, we need something smarter
  private[this] val associations = new ConcurrentHashMap[Address, Association]()

  override def start(): Unit = {
    // TODO: Configure materializer properly
    // TODO: Have a supervisor actor
    localAddress = UniqueAddress(
      Address("akka.artery", system.name, remoteSettings.ArteryHostname, remoteSettings.ArteryPort),
      AddressUidExtension(system).addressUid)
    materializer = ActorMaterializer()(system)

    transport =
      new Transport(
        localAddress,
        this, //FIXME should we collapse ArterySubsystem and Transport into one class?
        system,
        materializer,
        provider,
        AkkaPduProtobufCodec)
    transport.start()
  }

  override def shutdown(): Future[Done] = {
    if (transport != null) transport.shutdown()
    else Future.successful(Done)
  }

  def sendReply(to: Address, message: AnyRef) = {
    val association = associate(to)
    send(message, None, association.dummyRecipient)
  }

  override def send(message: Any, senderOption: Option[ActorRef], recipient: RemoteActorRef): Unit = {
    val cached = recipient.cachedAssociation
    val remoteAddress = recipient.path.address

    val association =
      if (cached ne null) cached
      else associate(remoteAddress)

    association.send(message, senderOption, recipient)
  }

  def associate(remoteAddress: Address): Association = {
    val current = associations.get(remoteAddress)
    if (current ne null) current
    else {
      associations.computeIfAbsent(remoteAddress, new JFunction[Address, Association] {
        override def apply(remoteAddress: Address): Association = {
          val newAssociation = new Association(materializer, remoteAddress, transport)
          newAssociation.associate() // This is a bit costly for this blocking method :(
          newAssociation
        }
      })
    }
  }

  override def quarantine(remoteAddress: Address, uid: Option[Int]): Unit = {
    ???
  }

}

/**
 * INTERNAL API
 * Outbound association API that is used by the stream stages.
 * Separate trait to facilitate testing without real transport.
 */
private[akka] trait OutboundContext {
  def localAddress: UniqueAddress
  def remoteAddress: Address

  def uniqueRemoteAddress: Future[UniqueAddress]
  def completeRemoteAddress(a: UniqueAddress): Unit

  def replySubject: ReplySubject

  // FIXME we should be able to Send without a recipient ActorRef
  def dummyRecipient: RemoteActorRef
}

/**
 * INTERNAL API
 *
 * Thread-safe, mutable holder for association state. Main entry point for remote destined message to a specific
 * remote address.
 */
private[akka] class Association(
  val materializer: Materializer,
  override val remoteAddress: Address,
  val transport: Transport) extends OutboundContext {

  @volatile private[this] var queue: SourceQueueWithComplete[Send] = _
  @volatile private[this] var systemMessageQueue: SourceQueueWithComplete[Send] = _

  override def localAddress: UniqueAddress = transport.localAddress

  // FIXME we also need to be able to switch to new uid
  private val _uniqueRemoteAddress = Promise[UniqueAddress]()
  override def uniqueRemoteAddress: Future[UniqueAddress] = _uniqueRemoteAddress.future
  override def completeRemoteAddress(a: UniqueAddress): Unit = {
    require(a.address == remoteAddress, s"Wrong UniqueAddress got [$a.address], expected [$remoteAddress]")
    _uniqueRemoteAddress.trySuccess(a)
  }

  override def replySubject: ReplySubject = transport.replySubject

  // FIXME we should be able to Send without a recipient ActorRef
  override val dummyRecipient: RemoteActorRef =
    transport.provider.resolveActorRef(RootActorPath(remoteAddress) / "system" / "dummy").asInstanceOf[RemoteActorRef]

  def send(message: Any, senderOption: Option[ActorRef], recipient: RemoteActorRef): Unit = {
    // TODO: lookup subchannel
    // FIXME: Use a different envelope than the old Send, but make sure the new is handled by deadLetters properly
    message match {
      case _: SystemMessage | _: Reply ⇒
        implicit val ec = materializer.executionContext
        systemMessageQueue.offer(Send(message, senderOption, recipient, None)).onFailure {
          case e ⇒
            // FIXME proper error handling, and quarantining
            println(s"# System message dropped, due to $e") // FIXME
        }
      case _ ⇒
        queue.offer(Send(message, senderOption, recipient, None))
    }
  }

  def quarantine(uid: Option[Int]): Unit = ()

  // Idempotent
  def associate(): Unit = {
    // FIXME detect and handle stream failure, e.g. handshake timeout
    if (queue eq null)
      queue = Source.queue(256, OverflowStrategy.dropBuffer)
        .to(transport.outbound(this)).run()(materializer)
    if (systemMessageQueue eq null)
      systemMessageQueue = Source.queue(256, OverflowStrategy.dropBuffer)
        .to(transport.outboundSystemMessage(this)).run()(materializer)
  }
}

