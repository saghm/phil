mod display;
mod error;

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use phil_core::cluster::{Cluster, ClusterOptions, Credential, TlsOptions, Topology};
use structopt::StructOpt;
use uuid::Uuid;

use crate::{display::ClientOptionsWrapper, error::Result};

fn create_tempdir() -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("phil-mongodb-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir)?;

    Ok(dir)
}

fn create_tempfile() -> Result<PathBuf> {
    let path = std::env::temp_dir().join(format!("phil-keyfile-{}", Uuid::new_v4()));
    std::fs::write(&path, &"phil and ravi")?;

    if cfg!(unix) {
        use std::{fs::Permissions, os::unix::fs::PermissionsExt};

        std::fs::set_permissions(&path, Permissions::from_mode(0o600))?;
    }

    Ok(path)
}

#[derive(Debug, StructOpt)]
#[structopt(about, author)]
struct Options {
    /// the topology type of the cluster to start
    #[structopt(name = "TOPOLOGY", possible_values(&["single", "replset", "sharded"]))]
    topology: String,

    /// the ID of the database version managed by monger to use
    #[structopt(name = "ID")]
    id: String,

    /// the number of nodes per replica set
    #[structopt(long, short, requires_if("topology", "replset"))]
    nodes: Option<u8>,

    /// the name of the replica set
    #[structopt(long, short, requires_if("topology", "replset"))]
    set_name: Option<String>,

    /// the number of mongos routers to start
    #[structopt(long, requires_if("topology", "sharded"))]
    num_mongos: Option<u8>,

    /// the number of shards to start
    #[structopt(long, requires_if("topology", "sharded"))]
    num_shards: Option<u8>,

    /// what type of shards to start
    #[structopt(
		long,
		requires_if("topology", "sharded"),
		possible_values(&["single", "replset"]),
		default_value_if("topology", Some("sharded"), "replset"),
	)]
    shard_type: Option<String>,

    /// enable (and require) TLS for the cluster
    #[structopt(long)]
    tls: bool,

    /// allow clients to connect to the server without TLS certificates
    #[structopt(long, requires("tls"))]
    allow_clients_without_certs: bool,

    /// the certificate authority file to use for TLS (defaults to ./ca.pem)
    #[structopt(long, requires("tls"))]
    ca_file: Option<String>,

    /// the server private key certificate file to use for TLS (defaults to ./server.pem)
    #[structopt(long, requires("tls"))]
    server_cert_file: Option<String>,

    /// the client private key certificate file to use for TLS (needed to initialize the cluster
    /// when client certificates are required); defaults to ./client.pem)
    #[structopt(long, requires("tls"))]
    client_cert_file: Option<String>,

    /// require authentication to connect to the cluster
    #[structopt(long)]
    auth: bool,

    /// log verbosely
    #[structopt(long, short)]
    verbose: bool,

    /// extra arguments for the mongod being run
    #[structopt(name = "MONGODB_ARGS", last(true))]
    mongod_args: Vec<String>,
}

fn main() -> Result<()> {
    let options = Options::from_args();

    let extra_mongod_args = options
        .mongod_args
        .into_iter()
        .map(OsString::from)
        .collect();

    let mut cluster_options = match options.topology.as_ref() {
        "single" => ClusterOptions::builder()
            .topology(Topology::Single)
            .version_id(options.id)
            .verbose(options.verbose)
            .extra_mongod_args(extra_mongod_args)
            .build(),
        "replset" => {
            let nodes = options.nodes.unwrap_or(3);
            let set_name = options.set_name.unwrap_or_else(|| "phil".into());
            let paths: Result<Vec<_>> = (0..nodes).map(|_| create_tempdir()).collect();

            ClusterOptions::builder()
                .topology(Topology::ReplicaSet {
                    db_paths: paths?,
                    set_name,
                })
                .version_id(options.id)
                .verbose(options.verbose)
                .extra_mongod_args(extra_mongod_args)
                .build()
        }
        "sharded" => {
            let num_shards = options.num_shards.unwrap_or(1);
            let num_mongos = options.num_mongos.unwrap_or(2);
            let replica_set_shards = options
                .shard_type
                .map(|shard_type| shard_type == "replset")
                .unwrap_or(true);

            let db_paths: Result<_> = (0..num_shards)
                .map(|_| {
                    if replica_set_shards {
                        Ok(vec![
                            create_tempdir()?,
                            create_tempdir()?,
                            create_tempdir()?,
                        ])
                    } else {
                        Ok(vec![create_tempdir()?])
                    }
                })
                .collect();

            ClusterOptions::builder()
                .topology(Topology::Sharded {
                    num_mongos,
                    shard_db_paths: db_paths?,
                    config_db_path: create_tempdir()?,
                })
                .version_id(options.id)
                .verbose(options.verbose)
                .extra_mongod_args(extra_mongod_args)
                .build()
        }
        _ => unreachable!(),
    };

    if options.tls {
        let ca_file_path =
            Path::new(options.ca_file.as_deref().unwrap_or("./ca.pem")).canonicalize()?;
        let server_cert_file_path = Path::new(
            options
                .server_cert_file
                .as_deref()
                .unwrap_or("./server.pem"),
        )
        .canonicalize()?;
        let client_cert_file_path = Path::new(
            options
                .client_cert_file
                .as_deref()
                .unwrap_or("./client.pem"),
        )
        .canonicalize()?;

        cluster_options.tls = Some(TlsOptions {
            weak_tls: options.allow_clients_without_certs,
            allow_invalid_certificates: true,
            ca_file_path,
            server_cert_file_path,
            client_cert_file_path,
        });
    }

    if options.auth {
        cluster_options.auth = Some(Credential {
            username: "phil".into(),
            password: "ravi".into(),
            key_file: create_tempfile()?,
        });
    }

    let cluster = Cluster::new(cluster_options)?;

    println!(
        "MONGODB_URI='{}'",
        ClientOptionsWrapper(cluster.client_options())
    );

    Ok(())
}
