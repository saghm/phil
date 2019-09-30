#[cfg(test)]
mod test;

use std::{collections::VecDeque, ffi::OsString, path::PathBuf};

use monger_core::{process::ChildType, Monger};
use mongodb::{
    bson::{bson, doc, Bson, Document},
    options::{ClientOptions, Host, TlsOptions as DriverTlsOptions},
    read_preference::ReadPreference,
    Client,
};
use serde::Deserialize;
use typed_builder::TypedBuilder;

use crate::error::{Error, Result};

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
    nodes: Vec<Host>,
    topology: Topology,
    tls: Option<TlsOptions>,
}

#[derive(Debug, TypedBuilder)]
pub struct ClusterOptions {
    pub topology: Topology,

    #[builder(default)]
    pub paths: Vec<PathBuf>,

    #[builder(default)]
    pub tls: Option<TlsOptions>,
}

#[derive(Debug)]
pub struct TlsOptions {
    pub allow_invalid_certificates: bool,
    pub ca_file_path: PathBuf,
    pub cert_file_path: PathBuf,
}

impl From<TlsOptions> for DriverTlsOptions {
    fn from(opts: TlsOptions) -> Self {
        Self::builder()
            .allow_invalid_certificates(opts.allow_invalid_certificates)
            .ca_file_path(opts.ca_file_path.to_string_lossy().into_owned())
            .build()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResponse {
    pub ok: f64,
    pub code_name: Option<String>,
}

fn add_tls_options(args: &mut Vec<OsString>, tls_options: Option<&TlsOptions>) {
    if let Some(tls_options) = tls_options {
        args.extend_from_slice(&[
            OsString::from("--tlsMode"),
            OsString::from("requireTLS"),
            OsString::from("--tlsCAFile"),
            OsString::from(&tls_options.ca_file_path),
            OsString::from("--tlsCertificateKeyFile"),
            OsString::from(&tls_options.cert_file_path),
            OsString::from("--tlsAllowConnectionsWithoutCertificates"),
        ]);
    }
}

impl Cluster {
    pub fn new(options: ClusterOptions) -> Result<Self> {
        match options.topology {
            Topology::Single => {
                Self::start_single_server(options.paths.into_iter().next(), options.tls)
            }
            Topology::ReplicaSet { nodes, set_name } => {
                Self::start_replica_set(nodes, set_name, options.paths, options.tls)
            }
            Topology::Sharded {
                replica_set_shards: false,
                ..
            } => unimplemented!(),
            Topology::Sharded {
                replica_set_shards: true,
                ..
            } => unimplemented!(),
        }
    }

    pub fn client_options(&self) -> &ClientOptions {
        &self.client_options
    }

    fn start_single_server(path: Option<PathBuf>, tls_options: Option<TlsOptions>) -> Result<Self> {
        let nodes = vec![Host::new("localhost".into(), Some(27017))];
        let monger = Monger::new()?;

        let mut args: Vec<_> = vec![
            OsString::from("--port".to_string()),
            OsString::from("27017".to_string()),
        ];

        if let Some(path) = path {
            args.push(OsString::from("--dbpath".to_string()));
            args.push(path.into_os_string());
        }

        add_tls_options(&mut args, tls_options.as_ref());
        monger.start_mongod(args, "4.2.0", ChildType::Fork)?;

        let client_options = ClientOptions::builder()
            .hosts(nodes.clone())
            .tls_options(tls_options.map(Into::into))
            .build();
        let client = Client::with_options(client_options.clone())?;

        Ok(Self {
            monger,
            client,
            client_options,
            nodes,
            topology: Topology::Single,
            tls: None,
        })
    }

    fn start_replica_set(
        nodes: u8,
        set_name: String,
        paths: Vec<PathBuf>,
        tls_options: Option<TlsOptions>,
    ) -> Result<Self> {
        let nodes: Vec<_> = (0..nodes)
            .map(|i| Host::new("localhost".into(), Some(27017 + i as u16)))
            .collect();
        let monger = Monger::new()?;

        let mut paths: VecDeque<_> = paths.into_iter().collect();

        for node in &nodes {
            let mut args = vec![
                OsString::from("--port".to_string()),
                OsString::from(node.port.unwrap().to_string()),
                OsString::from("--replSet"),
                OsString::from(&set_name),
            ];

            if let Some(path) = paths.pop_front() {
                args.push(OsString::from("--dbpath".to_string()));
                args.push(path.into_os_string());
            }

            add_tls_options(&mut args, tls_options.as_ref());
            monger.start_mongod(args, "4.2.0", ChildType::Fork)?;
        }

        let client_options = ClientOptions::builder()
            .hosts(nodes.clone())
            .repl_set_name(set_name.clone())
            .tls_options(tls_options.map(Into::into))
            .build();
        let client = Client::with_options(client_options.clone())?;

        let config = doc! {
            "_id": set_name.clone(),
            "members": nodes.iter().enumerate().map(|(i, host)| {
                Bson::Document(
                    doc! {
                        "_id": i as i32,
                        "host": host.to_string(),
                    }
                )
            }).collect::<Vec<_>>()
        };

        Self::configure_repl_set(&client, config)?;

        Ok(Self {
            topology: Topology::ReplicaSet {
                nodes: nodes.len() as u8,
                set_name,
            },
            monger,
            client,
            client_options,
            nodes,
            tls: None,
        })
    }

    fn configure_repl_set(client: &Client, config: Document) -> Result<()> {
        let db = client.database("admin");

        let response = db.run_command(
            doc! {
                "replSetInitiate": config.clone(),
            },
            Some(ReadPreference::Nearest {
                tag_sets: None,
                max_staleness: None,
            }),
        )?;

        let CommandResponse { ok, code_name } =
            mongodb::bson::from_bson(Bson::Document(response.clone()))?;

        if ok == 1.0 {
            return Ok(());
        }

        if let Some(code_name) = code_name {
            if code_name != "AlreadyInitialized" {
                return Err(Error::ReplicaSetConfigError { response });
            }
        }

        db.run_command(
            doc! {
                "replSetReconfig": config,
            },
            Some(ReadPreference::Nearest {
                tag_sets: None,
                max_staleness: None,
            }),
        )?;

        Ok(())
    }

    pub fn shutdown(self) {
        let _ = self
            .client
            .database("admin")
            .run_command(doc! { "shutdown": 1 }, None);
    }
}
