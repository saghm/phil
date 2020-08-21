#[cfg(test)]
mod test;

use std::{ffi::OsString, path::PathBuf};

use monger_core::Monger;
use mongodb::{
    options::{ClientOptions, Credential as DriverCredential, Tls, TlsOptions as DriverTlsOptions},
    sync::Client,
};
use typed_builder::TypedBuilder;

use crate::{
    error::Result,
    launch::{Launcher, Node},
};

#[derive(Debug, Clone)]
pub enum Topology {
    Single,
    ReplicaSet {
        set_name: String,
        db_paths: Vec<PathBuf>,
    },
    Sharded {
        num_mongos: u8,
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

    #[builder(default)]
    extra_mongod_args: Vec<OsString>,

    #[builder(default)]
    verbose: bool,
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
    pub key_file: PathBuf,
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
            options.verbose,
            options.extra_mongod_args,
        )?;

        launcher.initialize_cluster()
    }

    pub fn client_options(&self) -> &ClientOptions {
        &self.client_options
    }
}
