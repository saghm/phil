#[cfg(test)]
mod test;

use std::path::PathBuf;

use bson::{bson, doc};
use monger_core::Monger;
use mongodb::{
    options::{ClientOptions, StreamAddress, Tls, TlsOptions as DriverTlsOptions},
    Client,
};
use typed_builder::TypedBuilder;

use crate::{
    error::Result,
    launch::{self, ServerShardType},
};

#[derive(Debug, Clone)]
pub enum Topology {
    Single,
    ReplicaSet {
        nodes: u8,
        set_name: String,
    },
    Sharded {
        num_shards: u8,
        replica_set_shards: bool,
    },
}

#[derive(Debug)]
pub struct Cluster {
    monger: Monger,
    client: Client,
    client_options: ClientOptions,
    hosts: Vec<StreamAddress>,
    topology: Topology,
    tls: Option<TlsOptions>,
}

#[derive(Debug, TypedBuilder)]
pub struct ClusterOptions {
    pub topology: Topology,

    pub version_id: String,

    #[builder(default)]
    pub paths: Vec<PathBuf>,

    #[builder(default)]
    pub tls: Option<TlsOptions>,
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

impl Cluster {
    pub fn new(options: ClusterOptions) -> Result<Self> {
        match options.topology {
            Topology::Single => Self::start_single_server(
                options.version_id,
                options.paths.into_iter().next(),
                options.tls,
            ),
            Topology::ReplicaSet { nodes, set_name } => Self::start_replica_set(
                options.version_id,
                nodes,
                set_name,
                options.paths,
                options.tls,
            ),
            Topology::Sharded {
                num_shards,
                replica_set_shards: false,
            } => Self::start_singleton_shards(
                options.version_id,
                num_shards,
                options.paths,
                options.tls,
            ),
            Topology::Sharded {
                num_shards,
                replica_set_shards: true,
            } => Self::start_replset_shards(
                options.version_id,
                num_shards,
                options.paths,
                options.tls,
            ),
        }
    }

    pub fn client_options(&self) -> &ClientOptions {
        &self.client_options
    }

    fn start_single_server(
        version_id: String,
        path: Option<PathBuf>,
        tls_options: Option<TlsOptions>,
    ) -> Result<Self> {
        let hosts = vec![launch::stream_address("localhost", 27017)];
        let monger = Monger::new()?;

        launch::single_server(
            &monger,
            &version_id,
            27017,
            path,
            tls_options.as_ref(),
            false,
        )?;

        let client_options = ClientOptions::builder()
            .hosts(hosts.clone())
            .tls(tls_options.map(Into::into))
            .build();
        let client = Client::with_options(client_options.clone())?;

        Ok(Self {
            monger,
            client,
            client_options,
            hosts,
            topology: Topology::Single,
            tls: None,
        })
    }

    fn start_replica_set(
        version_id: String,
        nodes: u8,
        set_name: String,
        paths: Vec<PathBuf>,
        tls_options: Option<TlsOptions>,
    ) -> Result<Self> {
        let hosts: Vec<_> = (0..nodes)
            .map(|i| launch::stream_address("localhost", 27017 + i as u16))
            .collect();
        let monger = Monger::new()?;

        let client = launch::replica_set(
            &monger,
            &version_id,
            hosts.clone(),
            &set_name,
            paths.into_iter(),
            tls_options.as_ref(),
            ServerShardType::None,
        )?;

        let client_options = ClientOptions::builder()
            .hosts(hosts.clone())
            .repl_set_name(set_name.clone())
            .tls(tls_options.map(Into::into))
            .build();

        Ok(Self {
            topology: Topology::ReplicaSet {
                nodes: hosts.len() as u8,
                set_name,
            },
            monger,
            client,
            client_options,
            hosts,
            tls: None,
        })
    }

    fn start_singleton_shards(
        version_id: String,
        num_shards: u8,
        paths: Vec<PathBuf>,
        tls_options: Option<TlsOptions>,
    ) -> Result<Self> {
        let monger = Monger::new()?;
        let mut paths = paths.into_iter();
        let mongos_port1 = 27017;
        let mongos_port2 = 27018;
        let config_port = 27019;

        let shard_ports = (0..num_shards).map(|i| 27020 + i as u16);
        for port in shard_ports.clone() {
            launch::single_server(
                &monger,
                &version_id,
                port,
                paths.next(),
                tls_options.as_ref(),
                true,
            )?;
        }

        launch::replica_set(
            &monger,
            &version_id,
            vec![launch::stream_address("localhost", config_port)],
            "phil-config-server",
            std::iter::empty(),
            tls_options.as_ref(),
            ServerShardType::Config,
        )?;

        launch::mongos(
            &monger,
            &version_id,
            mongos_port1,
            config_port,
            "phil-config-server",
            shard_ports.clone(),
            tls_options.as_ref(),
            None,
        )?;
        launch::mongos(
            &monger,
            &version_id,
            mongos_port2,
            config_port,
            "phil-config-server",
            shard_ports,
            tls_options.as_ref(),
            None,
        )?;

        let hosts = vec![
            launch::stream_address("localhost", mongos_port1),
            launch::stream_address("localhost", mongos_port2),
        ];

        let client_options = ClientOptions::builder()
            .hosts(hosts.clone())
            .tls(tls_options.map(Into::into))
            .build();
        let client = Client::with_options(client_options.clone())?;

        Ok(Self {
            topology: Topology::Sharded {
                num_shards,
                replica_set_shards: false,
            },
            monger,
            hosts,
            client,
            client_options,
            tls: None,
        })
    }

    fn start_replset_shards(
        version_id: String,
        num_shards: u8,
        paths: Vec<PathBuf>,
        tls_options: Option<TlsOptions>,
    ) -> Result<Self> {
        let monger = Monger::new()?;
        let mut paths = paths.into_iter();
        let mongos_port1 = 27017;
        let mongos_port2 = 27018;
        let config_port = 27019;

        let shard_ports = (0..num_shards).map(|i| 27020 + (3 * i) as u16);
        let shard_names: Vec<_> = (0..num_shards)
            .map(|i| format!("phil-replset-shard-{}", i))
            .collect();
        for (i, port) in shard_ports.clone().enumerate() {
            launch::replica_set(
                &monger,
                &version_id,
                (port..port + 3)
                    .map(|p| launch::stream_address("localhost", p))
                    .collect(),
                &shard_names[i],
                &mut paths,
                tls_options.as_ref(),
                ServerShardType::Shard,
            )?;
        }

        launch::replica_set(
            &monger,
            &version_id,
            vec![launch::stream_address("localhost", config_port)],
            "phil-config-server",
            std::iter::empty(),
            tls_options.as_ref(),
            ServerShardType::Config,
        )?;

        launch::mongos(
            &monger,
            &version_id,
            mongos_port1,
            config_port,
            "phil-config-server",
            shard_ports.clone(),
            tls_options.as_ref(),
            Some(shard_names.clone()),
        )?;
        launch::mongos(
            &monger,
            &version_id,
            mongos_port2,
            config_port,
            "phil-config-server",
            shard_ports,
            tls_options.as_ref(),
            Some(shard_names),
        )?;

        let hosts = vec![
            launch::stream_address("localhost", mongos_port1),
            launch::stream_address("localhost", mongos_port2),
        ];

        let client_options = ClientOptions::builder()
            .hosts(hosts.clone())
            .tls(tls_options.map(Into::into))
            .build();
        let client = Client::with_options(client_options.clone())?;

        Ok(Self {
            topology: Topology::Sharded {
                num_shards,
                replica_set_shards: false,
            },
            monger,
            hosts,
            client,
            client_options,
            tls: None,
        })
    }

    pub fn shutdown(self) {
        let _ = self
            .client
            .database("admin")
            .run_command(doc! { "shutdown": 1 }, None);
    }
}
