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

use crate::{error::Result, launch::Launcher};

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
    pub fn new(options: ClusterOptions) -> Result<Self> {
        let launcher = Launcher::new(
            options.topology,
            options.version_id,
            options.tls,
            options.auth,
        )?;

        launcher.initialize_cluster()
    }

    pub fn client_options(&self) -> &ClientOptions {
        &self.client_options
    }
}
