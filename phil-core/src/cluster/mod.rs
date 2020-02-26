#[cfg(test)]
mod test;

use std::{path::PathBuf, process::Child};

use monger_core::Monger;
use mongodb::{
    options::{
        auth::Credential as DriverCredential,
        ClientOptions,
        StreamAddress,
        Tls,
        TlsOptions as DriverTlsOptions,
    },
    Client,
};
use typed_builder::TypedBuilder;

#[derive(Debug, Clone)]
pub enum Topology {
    Single,
    ReplicaSet {
        set_name: String,
        db_paths: Vec<PathBuf>,
    },
    Sharded {
        shard_db_paths: Vec<Vec<PathBuf>>,
        config_db_path: PathBuf,
    },
}

#[derive(Debug)]
pub struct Cluster {
    pub(crate) monger: Monger,
    pub(crate) client: Client,
    pub(crate) client_options: ClientOptions,
    pub(crate) topology: Topology,
    pub(crate) tls: Option<TlsOptions>,
    pub(crate) auth: Option<Credential>,
    pub(crate) nodes: Vec<Node>,
}

#[derive(Clone, Debug, TypedBuilder)]
pub struct ClusterOptions {
    pub topology: Topology,

    pub version_id: String,

    #[builder(default)]
    pub paths: Vec<PathBuf>,

    #[builder(default)]
    pub tls: Option<TlsOptions>,

    #[builder(default)]
    pub auth: Option<Credential>,
}

#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) address: StreamAddress,
    pub(crate) process: Child,
    pub(crate) db_path: Option<PathBuf>,
    pub(crate) repl_set_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TlsOptions {
    pub weak_tls: bool,
    pub allow_invalid_certificates: bool,
    pub ca_file_path: PathBuf,
    pub server_cert_file_path: PathBuf,
    pub client_cert_file_path: PathBuf,
}

impl From<TlsOptions> for Tls {
    fn from(opts: TlsOptions) -> Self {
        DriverTlsOptions::builder()
            .allow_invalid_certificates(opts.allow_invalid_certificates)
            .ca_file_path(opts.ca_file_path.to_string_lossy().into_owned())
            .cert_key_file_path(opts.server_cert_file_path.to_string_lossy().into_owned())
            .build()
            .into()
    }
}

#[derive(Debug, Clone)]
pub struct Credential {
    pub username: String,
    pub password: String,
}

impl From<Credential> for DriverCredential {
    fn from(credential: Credential) -> Self {
        Self::builder()
            .username(credential.username)
            .password(credential.password)
            .build()
    }
}

impl Cluster {
    //     pub fn new(mut options: ClusterOptions) -> Result<Self> {
    //         let credential = match options.auth.take() {
    //             Some(credential) => credential,
    //             None => return Self::initialize(options),
    //         };

    //         // Start cluster without auth.
    //         let cluster = Self::initialize(options.clone())?;

    //         // Add user to cluster.
    //         cluster.client.database("admin").run_command(
    //             doc! {
    //                 "createUser": credential.username.clone(),
    //                 "pwd": credential.password.clone(),
    //                 "roles": ["root"]
    //             },
    //             None,
    //         )?;

    //         // Shut down the cluster.
    //         cluster.shutdown()?;

    //         // Start up the cluster again with auth.
    //         options.auth = Some(credential);
    //         Self::initialize(options)
    //     }

    //     fn initialize(options: ClusterOptions) -> Result<Self> {
    //         match options.topology {
    //             Topology::Single => Self::start_single_server(
    //                 options.version_id,
    //                 options.paths.into_iter().next(),
    //                 options.tls,
    //                 options.auth.is_some(),
    //             ),
    //             Topology::ReplicaSet { nodes, set_name } => Self::start_replica_set(
    //                 options.version_id,
    //                 nodes,
    //                 set_name,
    //                 options.paths,
    //                 options.tls,
    //                 options.auth,
    //             ),
    //             Topology::Sharded {
    //                 num_shards,
    //                 replica_set_shards: false,
    //             } => Self::start_singleton_shards(
    //                 options.version_id,
    //                 num_shards,
    //                 options.paths,
    //                 options.tls,
    //                 options.auth,
    //             ),
    //             Topology::Sharded {
    //                 num_shards,
    //                 replica_set_shards: true,
    //             } => Self::start_replset_shards(
    //                 options.version_id,
    //                 num_shards,
    //                 options.paths,
    //                 options.tls,
    //                 options.auth,
    //             ),
    //         }
    //     }

    //     pub fn client_options(&self) -> &ClientOptions {
    //         &self.client_options
    //     }

    //     fn start_single_server(
    //         version_id: String,
    //         db_path: Option<PathBuf>,
    //         tls_options: Option<TlsOptions>,
    //         auth: bool,
    //     ) -> Result<Self> {
    //         let address = launch::stream_address("localhost", 27017);
    //         let monger = Monger::new()?;

    //         let process = launch::single_server(
    //             &monger,
    //             &version_id,
    //             27017,
    //             db_path.clone(),
    //             tls_options.as_ref(),
    //             false,
    //             auth,
    //         )?;

    //         let client_options = ClientOptions::builder()
    //             .hosts(vec![address.clone()])
    //             .tls(tls_options.map(Into::into))
    //             .build();
    //         let client = Client::with_options(client_options.clone())?;

    //         Ok(Self {
    //             monger,
    //             client,
    //             client_options,
    //             topology: Topology::Single,
    //             tls: None,
    //             auth,
    //             nodes: vec![Node {
    //                 address,
    //                 process,
    //                 db_path,
    //             }],
    //         })
    //     }

    //     fn start_replica_set(
    //         version_id: String,
    //         nodes: u8,
    //         set_name: String,
    //         paths: Vec<PathBuf>,
    //         tls_options: Option<TlsOptions>,
    //         auth: Option<Credential>,
    //     ) -> Result<Self> {
    //         let auth = auth.map(Into::into);

    //         let hosts: Vec<_> = (0..nodes)
    //             .map(|i| launch::stream_address("localhost", 27017 + i as u16))
    //             .collect();
    //         let monger = Monger::new()?;

    //         let mut processes = Vec::new();

    //         let client = launch::replica_set(
    //             &monger,
    //             &mut processes,
    //             &version_id,
    //             hosts.clone(),
    //             &set_name,
    //             paths.iter().cloned(),
    //             tls_options.as_ref(),
    //             ServerShardType::None,
    //             auth.clone(),
    //         )?;

    //         let client_options = ClientOptions::builder()
    //             .hosts(hosts.clone())
    //             .repl_set_name(set_name.clone())
    //             .tls(tls_options.map(Into::into))
    //             .credential(auth)
    //             .build();

    //         Ok(Self {
    //             topology: Topology::ReplicaSet {
    //                 nodes: hosts.len() as u8,
    //                 set_name,
    //             },
    //             monger,
    //             client,
    //             client_options,
    //             tls: None,
    //             auth: auth.is_some(),
    //             nodes: hosts
    //                 .into_iter()
    //                 .zip(processes)
    //                 .zip(paths)
    //                 .map(|((address, process), db_path)| Node {
    //                     address,
    //                     process,
    //                     db_path: Some(db_path),
    //                 })
    //                 .collect(),
    //         })
    //     }

    //     fn start_singleton_shards(
    //         version_id: String,
    //         num_shards: u8,
    //         paths: Vec<PathBuf>,
    //         tls_options: Option<TlsOptions>,
    //         auth: Option<Credential>,
    //     ) -> Result<Self> {
    //         let auth = auth.map(Into::into);

    //         let monger = Monger::new()?;
    //         let mut processes = Vec::new();
    //         let mut paths = paths.into_iter();
    //         let mongos_port1 = 27017;
    //         let mongos_port2 = 27018;
    //         let config_port = 27019;

    //         let shard_ports = (0..num_shards).map(|i| 27020 + i as u16);
    //         for port in shard_ports.clone() {
    //             let process = launch::single_server(
    //                 &monger,
    //                 &mut &version_id,
    //                 port,
    //                 paths.next(),
    //                 tls_options.as_ref(),
    //                 true,
    //                 auth.is_some(),
    //             )?;

    //             processes.push(process);
    //         }

    //         launch::replica_set(
    //             &monger,
    //             &mut processes,
    //             &version_id,
    //             vec![launch::stream_address("localhost", config_port)],
    //             "phil-config-server",
    //             std::iter::empty(),
    //             tls_options.as_ref(),
    //             ServerShardType::Config,
    //             auth.clone(),
    //         )?;

    //         launch::mongos(
    //             &monger,
    //             &mut processes,
    //             &version_id,
    //             mongos_port1,
    //             config_port,
    //             "phil-config-server",
    //             shard_ports.clone(),
    //             tls_options.as_ref(),
    //             None,
    //             auth.clone(),
    //         )?;
    //         launch::mongos(
    //             &monger,
    //             &mut processes,
    //             &version_id,
    //             mongos_port2,
    //             config_port,
    //             "phil-config-server",
    //             shard_ports,
    //             tls_options.as_ref(),
    //             None,
    //             auth.clone(),
    //         )?;

    //         let hosts = vec![
    //             launch::stream_address("localhost", mongos_port1),
    //             launch::stream_address("localhost", mongos_port2),
    //         ];

    //         let client_options = ClientOptions::builder()
    //             .hosts(hosts.clone())
    //             .tls(tls_options.map(Into::into))
    //             .credential(auth.clone())
    //             .build();
    //         let client = Client::with_options(client_options.clone())?;

    //         Ok(Self {
    //             topology: Topology::Sharded {
    //                 num_shards,
    //                 replica_set_shards: false,
    //             },
    //             monger,
    //             client,
    //             client_options,
    //             tls: None,
    //             auth: auth.is_some(),
    //             nodes: hosts
    //                 .into_iter()
    //                 .zip(processes)
    //                 .zip(paths)
    //                 .map(|((address, process), db_path)| Node {
    //                     address,
    //                     process,
    //                     db_path: Some(db_path),
    //                 })
    //                 .collect(),
    //         })
    //     }

    //     fn start_replset_shards(
    //         version_id: String,
    //         num_shards: u8,
    //         paths: Vec<PathBuf>,
    //         tls_options: Option<TlsOptions>,
    //         auth: Option<Credential>,
    //     ) -> Result<Self> {
    //         let auth = auth.map(Into::into);

    //         let monger = Monger::new()?;
    //         let mut processes = Vec::new();
    //         let mut paths = paths.into_iter();
    //         let mongos_port1 = 27017;
    //         let mongos_port2 = 27018;
    //         let config_port = 27019;

    //         let shard_ports = (0..num_shards).map(|i| 27020 + (3 * i) as u16);
    //         let shard_names: Vec<_> = (0..num_shards)
    //             .map(|i| format!("phil-replset-shard-{}", i))
    //             .collect();
    //         for (i, port) in shard_ports.clone().enumerate() {
    //             launch::replica_set(
    //                 &monger,
    //                 &mut processes,
    //                 &version_id,
    //                 (port..port + 3)
    //                     .map(|p| launch::stream_address("localhost", p))
    //                     .collect(),
    //                 &shard_names[i],
    //                 &mut paths,
    //                 tls_options.as_ref(),
    //                 ServerShardType::Shard,
    //                 auth.clone(),
    //             )?;
    //         }

    //         launch::replica_set(
    //             &monger,
    //             &mut processes,
    //             &version_id,
    //             vec![launch::stream_address("localhost", config_port)],
    //             "phil-config-server",
    //             std::iter::empty(),
    //             tls_options.as_ref(),
    //             ServerShardType::Config,
    //             auth.clone(),
    //         )?;

    //         launch::mongos(
    //             &monger,
    //             &mut processes,
    //             &version_id,
    //             mongos_port1,
    //             config_port,
    //             "phil-config-server",
    //             shard_ports.clone(),
    //             tls_options.as_ref(),
    //             Some(shard_names.clone()),
    //             auth.clone(),
    //         )?;

    //         launch::mongos(
    //             &monger,
    //             &mut processes,
    //             &version_id,
    //             mongos_port2,
    //             config_port,
    //             "phil-config-server",
    //             shard_ports,
    //             tls_options.as_ref(),
    //             Some(shard_names),
    //             auth.clone(),
    //         )?;

    //         let hosts = vec![
    //             launch::stream_address("localhost", mongos_port1),
    //             launch::stream_address("localhost", mongos_port2),
    //         ];

    //         let client_options = ClientOptions::builder()
    //             .hosts(hosts.clone())
    //             .tls(tls_options.map(Into::into))
    //             .credential(auth.clone())
    //             .build();
    //         let client = Client::with_options(client_options.clone())?;

    //         Ok(Self {
    //             topology: Topology::Sharded {
    //                 num_shards,
    //                 replica_set_shards: false,
    //             },
    //             monger,
    //             client,
    //             client_options,
    //             tls: None,
    //             auth: auth.is_some(),
    //             nodes: hosts
    //                 .into_iter()
    //                 .zip(processes)
    //                 .zip(paths)
    //                 .map(|((address, process), db_path)| Node {
    //                     address,
    //                     process,
    //                     db_path: Some(db_path),
    //                 })
    //                 .collect(),
    //         })
    //     }
}
