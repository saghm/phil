use std::{ffi::OsString, path::PathBuf, process::Child, time::Duration};

use bson::{bson, doc, Bson, Document};
use monger_core::Monger;
use mongodb::{
    options::{auth::Credential as DriverCredential, ClientOptions, StreamAddress, Tls},
    Client,
};
use serde::Deserialize;

use crate::{
    cluster::{Cluster, Credential, Node, TlsOptions, Topology},
    error::{Error, Result},
};

pub(crate) fn stream_address(host: &str, port: u16) -> StreamAddress {
    StreamAddress {
        hostname: host.into(),
        port: port.into(),
    }
}

#[derive(Debug)]
pub(crate) struct Launcher {
    monger: Monger,
    topology: Topology,
    version: String,
    tls: Option<TlsOptions>,
    credential: Option<Credential>,
    nodes: Vec<Node>,
}

impl Launcher {
    fn new(
        topology: Topology,
        tls: Option<TlsOptions>,
        credential: Option<Credential>,
    ) -> Result<Self> {
        Ok(Self {
            monger: Monger::new()?,
            topology,
            tls,
            credential,
            nodes: Default::default(),
        })
    }

    fn add_node(&mut self, port: u16, db_path: Option<PathBuf>) -> Result<()> {
        let mut args: Vec<OsString> = vec!["--port".into(), port.to_string().into()];

        if let Some(path) = db_path {
            args.push("--dbpath".into());
            args.push(path.into_os_string());
        }

        match self.topology {
            Topology::Single => {}
            Topology::ReplicaSet { ref set_name, .. } => {
                args.push("--replSet".into());
                args.push(set_name.clone().into());
            }
            Topology::Sharded {
                ref replica_set_name,
                ..
            } => {
                args.push("--shardsvr".into());

                if let Some(ref set_name) = replica_set_name {
                    args.push("--replSet".into());
                    args.push(set_name.clone().into());
                }
            }
        }

        if self.credential.is_some() {
            args.push(OsString::from("--auth"));
        }

        if let Some(ref tls_options) = self.tls {
            args.extend_from_slice(&[
                OsString::from("--tlsMode"),
                OsString::from("requireTLS"),
                OsString::from("--tlsCAFile"),
                OsString::from(&tls_options.ca_file_path),
                OsString::from("--tlsCertificateKeyFile"),
                OsString::from(&tls_options.server_cert_file_path),
            ]);

            if tls_options.weak_tls {
                args.push(OsString::from("--tlsAllowConnectionsWithoutCertificates"));
            }
        }

        let process = self.monger.start_mongod(args, &self.version, false)?;
        let node = Node {
            address: StreamAddress {
                hostname: "localhost".into(),
                port: Some(port),
            },
            process,
            db_path,
        };

        self.nodes.push(node);
    }

    fn repl_set_name(&self) -> Option<&str> {
        match self.topology {
            Topology::Single => None,
            Topology::ReplicaSet { ref set_name, .. } => Some(set_name),
            Topology::Sharded { .. } => None,
        }
    }

    fn client_options(&self) -> ClientOptions {
        ClientOptions::builder()
            .hosts(self.nodes.iter().map(|node| node.address.clone()).collect())
            .credential(self.credential.clone().into())
            .repl_set_name(self.repl_set_name())
            .tls(self.tls.into())
            .build()
    }

    fn configure_repl_set(&self, set_name: &str) -> Result<()> {
        let set_name = match self.repl_set_name() {
            Some(name) => name,
            None => return Ok(()),
        };

        let config = doc! {
            "_id": set_name,
            "members": self.nodes.iter().enumerate().map(|(i, node)| {
                Bson::Document(
                    doc! {
                        "_id": i as i32,
                        "host": node.address.to_string(),
                    }
                )
            }).collect::<Vec<_>>()
        };

        let mut options = self.client_options();
        options.direct_connection = Some(true);
        options.repl_set_name.take();

        let client = Client::with_options(options)?;

        let db = client.database("admin");
        let mut cmd = doc! {
            "replSetInitiate": config.clone(),
        };
        let mut already_initialized = false;

        loop {
            let response = db.run_command(cmd.clone(), None);

            let response = match response {
                Ok(response) => response,
                Err(..) => {
                    std::thread::sleep(Duration::from_millis(250));

                    continue;
                }
            };

            let CommandResponse { ok, code_name } =
                bson::from_bson(Bson::Document(response.clone()))?;

            if ok == 1.0 {
                break;
            }

            if let Some(code_name) = code_name {
                if code_name == "AlreadyInitialized" {
                    if !already_initialized {
                        cmd = doc! {
                            "replSetReconfig": config.clone(),
                        };
                    }

                    already_initialized = true;
                }
            }
        }

        loop {
            let response = db.run_command(doc! { "replSetGetStatus": 1 }, None);
            let response = match response {
                Ok(response) => response,
                Err(..) => {
                    std::thread::sleep(Duration::from_millis(250));

                    continue;
                }
            };

            let ReplSetStatus { members } = bson::from_bson(Bson::Document(response))?;

            if members.iter().any(|member| member.state_str == "PRIMARY") {
                return Ok(());
            }

            std::thread::sleep(Duration::from_millis(250));
        }
    }

    fn initialize_cluster(self) -> Result<Cluster> {
        let client_options = self.client_options();

        match self.topology {
            Topology::Single => {}
            Topology::ReplicaSet { ref set_name } => {
                self.configure_repl_set()?;
            }
            Topology::Sharded {
                ref replica_set_name,
            } => {}
        }

        let cluster = Cluster {
            monger: self.monger,
            client: Client::with_options(client_options.clone())?,
            client_options,
            topology: Topology::Single,
            tls: self.tls,
            auth: self.credential,
            nodes: self.nodes,
        };

        Ok(cluster)
    }
}

#[derive(Debug)]
pub(crate) enum ServerShardType {
    Config,
    #[allow(unused)]
    Shard,
    None,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommandResponse {
    pub ok: f64,
    pub code_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplSetStatus {
    members: Vec<ReplSetMember>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplSetMember {
    state_str: String,
}

pub(crate) fn mongos(
    monger: &Monger,
    processes: &mut Vec<Child>,
    version_id: &str,
    port: u16,
    config_port: u16,
    config_name: &str,
    shard_ports: impl IntoIterator<Item = u16>,
    tls_options: Option<&TlsOptions>,
    shard_names: Option<Vec<String>>,
    auth: Option<DriverCredential>,
) -> Result<Client> {
    let mut args = vec![
        OsString::from("--port"),
        OsString::from(port.to_string()),
        OsString::from("--configdb"),
        OsString::from(format!("{}/localhost:{}", config_name, config_port)),
    ];

    if auth.is_some() {
        args.push(OsString::from("--auth"));
    }

    processes.push(monger.run_background_command("mongos", args, version_id)?);

    let mut options = ClientOptions::builder()
        .hosts(vec![stream_address("localhost".into(), port)])
        .credential(auth)
        .build();

    if let Some(tls_options) = tls_options {
        options.tls = Some(Tls::from(tls_options.clone()));
    }

    let client = Client::with_options(options)?;

    let mut shard_names = shard_names.map(IntoIterator::into_iter);

    for shard_port in shard_ports {
        let shard_name = match shard_names.as_mut().and_then(Iterator::next) {
            Some(name) => format!("{}/localhost:{}", name, shard_port),
            None => format!("localhost:{}", shard_port),
        };

        let response = client
            .database("admin")
            .run_command(doc! { "addShard": shard_name }, None)?;

        let CommandResponse { ok, .. } = bson::from_bson(Bson::Document(response.clone()))?;

        if ok != 1.0 {
            return Err(Error::AddShardError { response });
        }
    }

    Ok(client)
}
