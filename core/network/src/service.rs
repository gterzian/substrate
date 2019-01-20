// Copyright 2017-2018 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::io;
use futures::{Async, Future, Stream, stream, sync::mpsc, sync::oneshot};
use parking_lot::Mutex;
use network_libp2p::{ProtocolId, PeerId, NetworkConfiguration, NodeIndex, ErrorKind, Severity};
use network_libp2p::{start_service, parse_str_addr, Service as NetworkService, ServiceEvent as NetworkServiceEvent};
use network_libp2p::{Protocol as Libp2pProtocol, RegisteredProtocol};
use consensus::import_queue::{ImportQueue, Link};
use consensus_gossip::ConsensusMessage;
use protocol::{self, Protocol, ProtocolMsg, ProtocolStatus, PeerInfo};
use codec::Decode;
use config::Params;
use crossbeam_channel::{unbounded, Receiver, Sender, TryRecvError};
use error::Error;
use runtime_primitives::traits::{Block as BlockT, NumberFor};
use specialization::NetworkSpecialization;

use tokio::prelude::task::AtomicTask;
use tokio::runtime::TaskExecutor;

/// Type that represents fetch completion future.
pub type FetchFuture = oneshot::Receiver<Vec<u8>>;

/// Sync status
pub trait SyncProvider<B: BlockT>: Send + Sync {
	/// Get sync status
	fn status(&self) -> ProtocolStatus<B>;
	/// Get currently connected peers
	fn peers(&self) -> Vec<(NodeIndex, Option<PeerId>, PeerInfo<B>)>;
}

/// Minimum Requirements for a Hash within Networking
pub trait ExHashT:
	::std::hash::Hash + Eq + ::std::fmt::Debug + Clone + Send + Sync + 'static
{
}
impl<T> ExHashT for T where
	T: ::std::hash::Hash + Eq + ::std::fmt::Debug + Clone + Send + Sync + 'static
{
}

/// Transaction pool interface
pub trait TransactionPool<H: ExHashT, B: BlockT>: Send + Sync {
	/// Get transactions from the pool that are ready to be propagated.
	fn transactions(&self) -> Vec<(H, B::Extrinsic)>;
	/// Import a transaction into the pool.
	fn import(&self, transaction: &B::Extrinsic) -> Option<H>;
	/// Notify the pool about transactions broadcast.
	fn on_broadcasted(&self, propagations: HashMap<H, Vec<String>>);
}

/// A link implementation that connects to the network.
pub struct NetworkLink<B: BlockT> {
	/// The protocol sender
	pub(crate) protocol_sender: Sender<ProtocolMsg<B>>,
	/// The network sender
	pub(crate) network_sender: NetworkChan,
}

impl<B: BlockT> Link<B> for NetworkLink<B> {
	fn block_imported(&self, hash: &B::Hash, number: NumberFor<B>) {
		let _ = self.protocol_sender.send(ProtocolMsg::BlockImportedSync(hash.clone(), number));
	}

	fn maintain_sync(&self) {
		let _ = self.protocol_sender.send(ProtocolMsg::MaintainSync);
	}

	fn useless_peer(&self, who: NodeIndex, reason: &str) {
		trace!(target:"sync", "Useless peer {}, {}", who, reason);
		self.network_sender.send(NetworkMsg::ReportPeer(who, Severity::Useless(reason.to_string())));
	}

	fn note_useless_and_restart_sync(&self, who: NodeIndex, reason: &str) {
		trace!(target:"sync", "Bad peer {}, {}", who, reason);
		// is this actually malign or just useless?
		self.network_sender.send(NetworkMsg::ReportPeer(who, Severity::Useless(reason.to_string())));
		let _ = self.protocol_sender.send(ProtocolMsg::RestartSync);
	}

	fn restart(&self) {
		let _ = self.protocol_sender.send(ProtocolMsg::RestartSync);
	}
}

/// Substrate network service. Handles network IO and manages connectivity.
pub struct Service<B: BlockT + 'static> {
	/// Network service
	network: Arc<Mutex<NetworkService>>,
	/// Protocol sender
	protocol_sender: Sender<ProtocolMsg<B>>,
	/// Network sender
	network_sender: NetworkChan,
}

impl<B: BlockT + 'static> Service<B> {
	/// Creates and register protocol with the network service
	pub fn new<I: 'static + ImportQueue<B>, S: NetworkSpecialization<B>, H: ExHashT>(
		task_executor: TaskExecutor,
		params: Params<B, S, H>,
		protocol_id: ProtocolId,
		import_queue: Arc<I>,
	) -> Result<Arc<Service<B>>, Error> {
		let (network_chan, network_port) = network_channel(protocol_id);
		let protocol_sender = Protocol::new(
			network_chan.clone(),
			params.config,
			params.chain,
			import_queue.clone(),
			params.on_demand,
			params.transaction_pool,
			params.specialization,
		)?;
		let versions = [(protocol::CURRENT_VERSION as u8)];
		let registered = RegisteredProtocol::new(protocol_id, &versions[..]);
		let network = start_network(
			task_executor,
			protocol_sender.clone(),
			network_port,
			network_chan.clone(),
			params.network_config,
			registered,
		)?;

		let service = Arc::new(Service {
			network,
			network_sender: network_chan.clone(),
			protocol_sender: protocol_sender.clone(),
		});

		// connect the import-queue to the network service.
		let link = NetworkLink {
			protocol_sender,
			network_sender: network_chan,
		};

		import_queue.start(link)?;

		Ok(service)
	}

	/// Get a clone of the channel to network/libp2p.
	pub fn network_sender(&self) -> NetworkChan {
		self.network_sender.clone()
	}

	/// Called when a new block is imported by the client.
	pub fn on_block_imported(&self, hash: B::Hash, header: &B::Header) {
		let _ = self
			.protocol_sender
			.send(ProtocolMsg::BlockImported(hash, header.clone()));
	}

	/// Called when new transactons are imported by the client.
	pub fn trigger_repropagate(&self) {
		let _ = self.protocol_sender.send(ProtocolMsg::PropagateExtrinsics);
	}

	/// Send a consensus message through the gossip
	pub fn gossip_consensus_message(&self, topic: B::Hash, message: Vec<u8>, broadcast: bool) {
		let _ = self
			.protocol_sender
			.send(ProtocolMsg::GossipConsensusMessage(
				topic, message, broadcast,
			));
	}

	/// access the underlying consensus gossip handler
	pub fn consensus_gossip_messages_for(
		&self,
		topic: B::Hash,
	) -> mpsc::UnboundedReceiver<ConsensusMessage> {
		let (sender, port) = unbounded();
		let _ = self
			.protocol_sender
			.send(ProtocolMsg::GossipConsensusMessagesFor(topic, sender));
		port.recv().unwrap()
	}

	/// Collect consensus gossip garbage for a topic.
	pub fn consensus_gossip_collect_garbage_for(&self, topic: B::Hash) {
		let _ = self
			.protocol_sender
			.send(ProtocolMsg::GossipConsensusCollectGarbargeFor(topic));
	}
}

impl<B: BlockT + 'static> ::consensus::SyncOracle for Service<B> {
	fn is_major_syncing(&self) -> bool {
		let (sender, port) = unbounded();
		let _ = self
			.protocol_sender
			.send(ProtocolMsg::IsMajorSyncing(sender));
		port.recv().unwrap()
	}
}

impl<B: BlockT + 'static> SyncProvider<B> for Service<B> {
	/// Get sync status
	fn status(&self) -> ProtocolStatus<B> {
		let (sender, port) = unbounded();
		let _ = self.protocol_sender.send(ProtocolMsg::Status(sender));
		port.recv().unwrap()
	}

	fn peers(&self) -> Vec<(NodeIndex, Option<PeerId>, PeerInfo<B>)> {
		let (sender, port) = unbounded();
		let _ = self.protocol_sender.send(ProtocolMsg::Peers(sender));
		let peers = port.recv().unwrap();
		let network = self.network.lock();
		peers.into_iter().map(|(idx, info)| {
			(idx, network.peer_id_of_node(idx).map(|p| p.clone()), info)
		}).collect::<Vec<_>>()
	}
}

impl<B: BlockT + 'static> Drop for Service<B> {
	fn drop(&mut self) {
		self.network_sender.send(NetworkMsg::Stop);
		let _ = self.protocol_sender.send(ProtocolMsg::Stop);
	}
}

/// Trait for managing network
pub trait ManageNetwork {
	/// Set to allow unreserved peers to connect
	fn accept_unreserved_peers(&self);
	/// Set to deny unreserved peers to connect
	fn deny_unreserved_peers(&self);
	/// Remove reservation for the peer
	fn remove_reserved_peer(&self, peer: PeerId);
	/// Add reserved peer
	fn add_reserved_peer(&self, peer: String) -> Result<(), String>;
	/// Returns a user-friendly identifier of our node.
	fn node_id(&self) -> Option<String>;
}

impl<B: BlockT + 'static> ManageNetwork for Service<B> {
	fn accept_unreserved_peers(&self) {
		self.network.lock().accept_unreserved_peers();
	}

	fn deny_unreserved_peers(&self) {
		self.network.lock().deny_unreserved_peers();
	}

	fn remove_reserved_peer(&self, peer: PeerId) {
		self.network.lock().remove_reserved_peer(peer);
	}

	fn add_reserved_peer(&self, peer: String) -> Result<(), String> {
		let (addr, peer_id) = parse_str_addr(&peer).map_err(|e| format!("{:?}", e))?;
		self.network.lock().add_reserved_peer(addr, peer_id);
		Ok(())
	}

	fn node_id(&self) -> Option<String> {
		let network = self.network.lock();
		let ret = network
			.listeners()
			.next()
			.map(|addr| {
				let mut addr = addr.clone();
				addr.append(Libp2pProtocol::P2p(network.peer_id().clone().into()));
				addr.to_string()
			});
		ret
	}
}

/// Create a NetworkPort/Chan pair.
pub fn network_channel(protocol_id: ProtocolId) -> (NetworkChan, NetworkPort) {
	let (network_sender, network_receiver) = unbounded();
	let task_notify = Arc::new(AtomicTask::new());
	let network_port = NetworkPort::new(network_receiver, protocol_id, task_notify.clone());
	let network_chan = NetworkChan::new(network_sender, task_notify);
	(network_chan, network_port)
}


/// A sender of NetworkMsg that notifies a task when a message has been sent.
#[derive(Clone)]
pub struct NetworkChan {
	sender: Sender<NetworkMsg>,
	task_notify: Arc<AtomicTask>,
}

impl NetworkChan {
	/// Create a new network chan.
	pub fn new(sender: Sender<NetworkMsg>, task_notify: Arc<AtomicTask>) -> Self {
		Self {
			sender,
			task_notify,
		}
	}

	/// Send a messaging, to be handled on a stream. Notify the task handling the stream.
	pub fn send(&self, msg: NetworkMsg) {
		let _ = self.sender.send(msg);
		self.task_notify.notify();
	}
}

impl Drop for NetworkChan {
	/// Notifying the task when a sender is dropped(when all are dropped, the stream is finished).
	fn drop(&mut self) {
		self.task_notify.notify();
	}
}


/// A receiver of NetworkMsg that makes the protocol-id available with each message.
pub struct NetworkPort {
	receiver: Receiver<NetworkMsg>,
	protocol_id: ProtocolId,
	task_notify: Arc<AtomicTask>,
	stopped: Cell<bool>,
}

impl NetworkPort {
	/// Create a new network port for a given protocol-id.
	pub fn new(receiver: Receiver<NetworkMsg>, protocol_id: ProtocolId, task_notify: Arc<AtomicTask>) -> Self {
		Self {
			receiver,
			protocol_id,
			task_notify,
			stopped: Cell::new(false),
		}
	}

	/// Receive a message, if any is currently-enqueued.
	/// Register the current tokio task for notification when a new message is available.
	pub fn take_one_message(&self) -> Result<Option<(ProtocolId, NetworkMsg)>, ()> {
		self.task_notify.register();
		match self.receiver.try_recv() {
			Ok(msg) => {
				if let NetworkMsg::Stop = msg {
					self.stopped.set(true);
				}
				return Ok(Some((self.protocol_id.clone(), msg)))
			},
			Err(TryRecvError::Empty) => {
				if self.stopped.get() {
					// In case we stopped already, this prevents the case
					// where the task is notified "too early" when the senders drop,
					// before the channel has disconnected, and the stream doesn't shut down.
					return Err(())
				}
			},
			Err(TryRecvError::Disconnected) => return Err(()),
		}
		Ok(None)
	}

	/// Get a reference to the underlying crossbeam receiver.
	pub fn receiver(&self) -> &Receiver<NetworkMsg> {
		&self.receiver
	}
}

/// Messages to be handled by NetworkService.
#[derive(Debug)]
pub enum NetworkMsg {
	/// Ask network to convert a list of nodes, to a list of peers.
	PeerIds(Vec<NodeIndex>, Sender<Vec<(NodeIndex, Option<PeerId>)>>),
	/// Send an outgoing custom message.
	Outgoing(NodeIndex, Vec<u8>),
	/// Report a peer.
	ReportPeer(NodeIndex, Severity),
	/// Get a peer id.
	GetPeerId(NodeIndex, Sender<Option<String>>),
	Stop,
}

/// Spawns the futures that handle the networking.
fn start_network<B: BlockT + 'static>(
	task_executor: TaskExecutor,
	protocol_sender: Sender<ProtocolMsg<B>>,
	network_port: NetworkPort,
	network_sender: NetworkChan,
	config: NetworkConfiguration,
	registered: RegisteredProtocol,
) -> Result<Arc<Mutex<NetworkService>>, Error> {
	let protocol_id = registered.id();

	// Start the main service.
	let service = match start_service(config, Some(registered)) {
		Ok(service) => Arc::new(Mutex::new(service)),
		Err(err) => {
			match err.kind() {
				ErrorKind::Io(ref e) if e.kind() == io::ErrorKind::AddrInUse =>
					warn!("Network port is already in use, make sure that another instance of Substrate client is not running or change the port using the --port option."),
				_ => warn!("Error starting network: {}", err),
			};
			return Err(err.into())
		},
	};

	let network_service = service.clone();
	let network_service_2 = service.clone();

	// Protocol produces a stream of messages about what happens in sync.
	let protocol = stream::poll_fn(move || {
		match network_port.take_one_message() {
			Ok(Some(message)) => Ok(Async::Ready(Some(message))),
			Ok(None) => Ok(Async::NotReady),
			Err(_) => Ok(Async::Ready(None)),
		}
	})
	.fuse()
	.for_each(move |(protocol_id, msg)| {
		// Handle message from Protocol.
		match msg {
			NetworkMsg::PeerIds(node_idxs, sender) => {
				let reply = node_idxs.into_iter().map(|idx| {
					(idx, network_service_2.lock().peer_id_of_node(idx).map(|p| p.clone()))
				}).collect::<Vec<_>>();
				let _ = sender.send(reply);
			}
			NetworkMsg::Outgoing(who, outgoing_message) => {
				network_service_2
					.lock()
					.send_custom_message(who, protocol_id, outgoing_message);
			},
			NetworkMsg::ReportPeer(who, severity) => {
				match severity {
					Severity::Bad(_) => network_service_2.lock().ban_node(who),
					Severity::Useless(_) => network_service_2.lock().drop_node(who),
					Severity::Timeout => network_service_2.lock().drop_node(who),
				}
			},
			NetworkMsg::GetPeerId(who, sender) => {
				let node_id = network_service_2
					.lock()
					.peer_id_of_node(who)
					.cloned()
					.map(|id| id.to_base58());
				let _ = sender.send(node_id);
			},
			NetworkMsg::Stop => network_service_2.lock().stop(),
		}
		Ok(())
	});

	// The network service produces events about what happens on the network. Let's process them.
	let network = stream::poll_fn(move || network_service.lock().poll())
	.fuse()
	.for_each(move |event| {
		match event {
			NetworkServiceEvent::ClosedCustomProtocols { node_index, protocols, debug_info } => {
				if !protocols.is_empty() {
					debug_assert_eq!(protocols, &[protocol_id]);
					let _ = protocol_sender.send(
						ProtocolMsg::PeerDisconnected(node_index, debug_info));
				}
			}
			NetworkServiceEvent::OpenedCustomProtocol { node_index, version, debug_info, .. } => {
				debug_assert_eq!(version, protocol::CURRENT_VERSION as u8);
				let _ = protocol_sender.send(ProtocolMsg::PeerConnected(node_index, debug_info));
			}
			NetworkServiceEvent::ClosedCustomProtocol { node_index, debug_info, .. } => {
				let _ = protocol_sender.send(ProtocolMsg::PeerDisconnected(node_index, debug_info));
			}
			NetworkServiceEvent::CustomMessage { node_index, data, .. } => {
				if let Some(m) = Decode::decode(&mut (&data as &[u8])) {
					let _ = protocol_sender.send(ProtocolMsg::CustomMessage(node_index, m));
					return Ok(())
				}
				network_sender.send(
					NetworkMsg::ReportPeer(
						node_index,
						Severity::Bad("Peer sent us a packet with invalid format".to_string())
					)
				);
			}
		}
		Ok(())
		}).map_err(move |_| ());

	task_executor.spawn(protocol);
	task_executor.spawn(network);

	Ok(service)
}
