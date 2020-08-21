mod display;

use std::{
    convert::{TryFrom, TryInto},
    ffi::OsString,
    path::{Path, PathBuf},
};

use anyhow::{Error, Result};
use phil_core::cluster::{Cluster, ClusterOptions, Credential, TlsOptions, Topology};
use self_update::backends::github::Update;
use structopt::StructOpt;
use uuid::Uuid;

use crate::display::ClientOptionsWrapper;

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
enum Command {
    /// start a single server
    Single {
        #[structopt(flatten)]
        options: SingleOptions,
    },

    /// start a replica set
    #[structopt(name = "replset")]
    ReplSet {
        #[structopt(flatten)]
        options: ReplSetOptions,
    },

    /// start a sharded cluster
    Sharded {
        #[structopt(flatten)]
        options: ShardedOptions,
    },

    /// updates phil to the latest version
    SelfUpdate,
}

#[derive(Debug, StructOpt)]
struct SingleOptions {
    #[structopt(flatten)]
    common: CommonOptions,
}

#[derive(Debug, StructOpt)]
struct ReplSetOptions {
    #[structopt(flatten)]
    common: CommonOptions,

    /// the number of nodes per replica set
    #[structopt(long, short, default_value = "3")]
    nodes: u8,

    /// the name of the replica set
    #[structopt(long, short, default_value = "phil")]
    set_name: String,
}

#[derive(Debug, StructOpt)]
struct ShardedOptions {
    #[structopt(flatten)]
    common: CommonOptions,

    /// the number of mongos routers to start
    #[structopt(long, default_value = "2")]
    num_mongos: u8,

    /// the number of shards to start
    #[structopt(long, default_value = "1")]
    num_shards: u8,

    /// what type of shards to start
    #[structopt(long, possible_values(&["single", "replset"]), default_value = "replset")]
    shard_type: String,
}

#[derive(Debug, StructOpt)]
struct CommonOptions {
    /// the ID of the database version managed by monger to use
    #[structopt(name = "ID")]
    id: String,

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

impl CommonOptions {
    fn tls_options(&self) -> Result<Option<TlsOptions>> {
        if !self.tls {
            return Ok(None);
        }

        let ca_file_path =
            Path::new(self.ca_file.as_deref().unwrap_or("./ca.pem")).canonicalize()?;
        let server_cert_file_path =
            Path::new(self.server_cert_file.as_deref().unwrap_or("./server.pem")).canonicalize()?;
        let client_cert_file_path =
            Path::new(self.client_cert_file.as_deref().unwrap_or("./client.pem")).canonicalize()?;

        Ok(Some(TlsOptions {
            weak_tls: self.allow_clients_without_certs,
            allow_invalid_certificates: true,
            ca_file_path,
            server_cert_file_path,
            client_cert_file_path,
        }))
    }

    fn auth_options(&self) -> Result<Option<Credential>> {
        if !self.auth {
            return Ok(None);
        }

        Ok(Some(Credential {
            username: "phil".into(),
            password: "ravi".into(),
            key_file: create_tempfile()?,
        }))
    }
}

impl TryFrom<SingleOptions> for ClusterOptions {
    type Error = Error;

    fn try_from(opts: SingleOptions) -> Result<Self> {
        Ok(ClusterOptions::builder()
            .topology(Topology::Single)
            .tls(opts.common.tls_options()?)
            .auth(opts.common.auth_options()?)
            .version_id(opts.common.id)
            .verbose(opts.common.verbose)
            .extra_mongod_args(
                opts.common
                    .mongod_args
                    .into_iter()
                    .map(OsString::from)
                    .collect(),
            )
            .build())
    }
}

impl TryFrom<ReplSetOptions> for ClusterOptions {
    type Error = Error;

    fn try_from(opts: ReplSetOptions) -> Result<Self> {
        let paths: Result<Vec<_>> = (0..opts.nodes).map(|_| create_tempdir()).collect();

        Ok(ClusterOptions::builder()
            .tls(opts.common.tls_options()?)
            .auth(opts.common.auth_options()?)
            .topology(Topology::ReplicaSet {
                set_name: opts.set_name,
                db_paths: paths?,
            })
            .version_id(opts.common.id)
            .verbose(opts.common.verbose)
            .extra_mongod_args(
                opts.common
                    .mongod_args
                    .into_iter()
                    .map(OsString::from)
                    .collect(),
            )
            .build())
    }
}

impl TryFrom<ShardedOptions> for ClusterOptions {
    type Error = Error;

    fn try_from(opts: ShardedOptions) -> Result<Self> {
        let db_paths: Result<_> = (0..opts.num_shards)
            .map(|_| {
                if opts.shard_type == "replset" {
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

        Ok(ClusterOptions::builder()
            .topology(Topology::Sharded {
                num_mongos: opts.num_mongos,
                shard_db_paths: db_paths?,
                config_db_path: create_tempdir()?,
            })
            .tls(opts.common.tls_options()?)
            .auth(opts.common.auth_options()?)
            .version_id(opts.common.id)
            .verbose(opts.common.verbose)
            .extra_mongod_args(
                opts.common
                    .mongod_args
                    .into_iter()
                    .map(OsString::from)
                    .collect(),
            )
            .build())
    }
}

fn main() -> Result<()> {
    let cluster_options = match Command::from_args() {
        Command::Single { options } => options.try_into()?,
        Command::ReplSet { options } => options.try_into()?,
        Command::Sharded { options } => options.try_into()?,
        Command::SelfUpdate => {
            let status = Update::configure()
                .repo_owner("saghm")
                .repo_name("phil")
                .current_version(env!("CARGO_PKG_VERSION"))
                .bin_name(env!("CARGO_PKG_NAME"))
                .show_download_progress(true)
                .build()?
                .update()?;

            if status.uptodate() {
                println!("Already have the latest version");
            } else {
                println!("Downloaded and installed {}", status.version());
            }

            return Ok(());
        }
    };

    let cluster = Cluster::new(cluster_options)?;

    println!(
        "MONGODB_URI='{}'",
        ClientOptionsWrapper(cluster.client_options())
    );

    Ok(())
}
