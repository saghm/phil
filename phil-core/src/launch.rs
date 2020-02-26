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

fn localhost_address(port: u16) -> StreamAddress {
    StreamAddress {
        hostname: "localhost".into(),
        port: Some(port),
    }
}

struct MongodOptions {
    port: u16,
    db_path: Option<PathBuf>,
    shard_server: bool,
    repl_set_name: Option<String>,
}

#[derive(Debug)]
pub(crate) struct Launcher {
    monger: Monger,
    topology: Topology,
    version: String,
    tls: Option<TlsOptions>,
    credential: Option<Credential>,
    nodes: Vec<Node>,
    next_port: u16,
    shard_count: u8,
}

impl Launcher {
    fn new(
        topology: Topology,
        version: String,
        tls: Option<TlsOptions>,
        credential: Option<Credential>,
    ) -> Result<Self> {
        Ok(Self {
            monger: Monger::new()?,
            topology,
            version,
            tls,
            credential,
            nodes: Default::default(),
            next_port: 27017,
            shard_count: 0,
        })
    }

    fn next_port(&mut self) -> u16 {
        let next_port = self.next_port + 1;
        std::mem::replace(&mut self.next_port, next_port)
    }

    fn next_shard_id(&mut self) -> u8 {
        let next_count = self.shard_count + 1;
        std::mem::replace(&mut self.shard_count, next_count)
    }

    fn repl_set_addresses(&self, repl_set_name: String) -> impl Iterator<Item = &StreamAddress> {
        self.nodes.iter().filter_map(move |node| {
            if node.repl_set_name == Some(repl_set_name.clone()) {
                Some(&node.address)
            } else {
                None
            }
        })
    }

    fn start_mongod(&mut self, options: MongodOptions) -> Result<Node> {
        let mut args: Vec<OsString> = vec!["--port".into(), self.next_port().to_string().into()];

        if let Some(ref path) = options.db_path {
            args.push("--dbpath".into());
            args.push(path.clone().into_os_string());
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
            address: localhost_address(options.port),
            process,
            db_path: options.db_path,
            repl_set_name: options.repl_set_name,
        };

        Ok(node)
    }

    fn configure_repl_set(&self, set_name: &str) -> Result<()> {
        let nodes: Vec<_> = self
            .repl_set_addresses(set_name.into())
            .enumerate()
            .map(|(i, address)| {
                Bson::Document(doc! {
                    "_id": i as i32,
                    "host": address.to_string(),
                })
            })
            .collect();

        let config = doc! {
            "_id": set_name,
            "members": nodes,
        };

        let options = ClientOptions::builder()
            .hosts(vec![self
                .repl_set_addresses(set_name.into())
                .next()
                .unwrap()
                .clone()])
            .tls(self.tls.clone().map(Into::into))
            .credential(self.credential.clone().map(Into::into))
            .direct_connection(true)
            .build();

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

    fn start_repl_set(
        &mut self,
        repl_set_name: &str,
        shard_server: bool,
        db_paths: Vec<PathBuf>,
    ) -> Result<()> {
        for db_path in db_paths {
            let options = MongodOptions {
                port: self.next_port(),
                db_path: Some(db_path),
                shard_server,
                repl_set_name: Some(repl_set_name.into()),
            };

            let node = self.start_mongod(options)?;
            self.nodes.push(node);
        }

        self.configure_repl_set(repl_set_name)?;

        Ok(())
    }

    fn add_config_db(&mut self, port: u16, name: &str, db_path: PathBuf) -> Result<()> {
        let config_db_options = MongodOptions {
            port,
            db_path: Some(db_path),
            shard_server: false,
            repl_set_name: Some(name.into()),
        };

        self.start_mongod(config_db_options)?;

        Ok(())
    }

    fn add_mongos(&self, port: u16, config_db_port: u16, config_db_name: &str) -> Result<()> {
        let mut args: Vec<OsString> = vec![
            "--port".into(),
            port.to_string().into(),
            "--configdb".into(),
            format!("{}/localhost:{}", config_db_name, config_db_port).into(),
        ];

        if self.credential.is_some() {
            args.push(OsString::from("--auth"));
        }

        self.monger
            .run_background_command("mongos", args, &self.version)?;

        Ok(())
    }

    fn add_singleton_shard(&mut self, port: u16, mongos_port: u16, db_path: PathBuf) -> Result<()> {
        let options = MongodOptions {
            port,
            db_path: Some(db_path),
            shard_server: true,
            repl_set_name: None,
        };

        self.start_mongod(options)?;

        let client_options = ClientOptions::builder()
            .hosts(vec![localhost_address(mongos_port)])
            .credential(self.credential.clone().map(Into::into))
            .tls(self.tls.clone().map(Into::into))
            .build();

        let client = Client::with_options(client_options)?;

        let name = format!("phil-replset-shard-{}", self.next_shard_id());

        let response = client.database("admin").run_command(
            doc! {
                "addShard": localhost_address(port).to_string(),
                "name": name
            },
            None,
        )?;

        let CommandResponse { ok, .. } = bson::from_bson(Bson::Document(response.clone()))?;

        if ok != 1.0 {
            return Err(Error::AddShardError { response });
        }

        Ok(())
    }

    fn add_replset_shard(&mut self, mongos_port: u16, db_paths: Vec<PathBuf>) -> Result<()> {
        let name = format!("phil-replset-shard-{}", self.next_shard_id());
        self.start_repl_set(&name, true, db_paths)?;

        let options = ClientOptions::builder()
            .hosts(vec![localhost_address(mongos_port)])
            .credential(self.credential.clone().map(Into::into))
            .tls(self.tls.clone().map(Into::into))
            .build();

        let client = Client::with_options(options)?;

        let node_addresses: Vec<_> = self
            .repl_set_addresses(name.clone())
            .map(|address| address.to_string())
            .collect();

        let response = client.database("admin").run_command(
            doc! {
                "addShard": format!("{}/{}", name, node_addresses.join(",")),
                "name": name
            },
            None,
        )?;

        let CommandResponse { ok, .. } = bson::from_bson(Bson::Document(response.clone()))?;

        if ok != 1.0 {
            return Err(Error::AddShardError { response });
        }

        Ok(())
    }

    fn initialize_cluster(mut self) -> Result<Cluster> {
        let mut client_options = ClientOptions::builder()
            .tls(self.tls.clone().map(Into::into))
            .credential(self.credential.clone().map(Into::into))
            .build();

        match self.topology.clone() {
            Topology::Single => {
                let options = MongodOptions {
                    port: 27017,
                    db_path: None,
                    shard_server: false,
                    repl_set_name: None,
                };

                let node = self.start_mongod(options)?;
                self.nodes.push(node);

                client_options.hosts = vec![localhost_address(27017)];
            }
            Topology::ReplicaSet { set_name, db_paths } => {
                self.start_repl_set(&set_name, false, db_paths.to_vec())?;

                client_options.hosts = (0..db_paths.len())
                    .into_iter()
                    .map(|i| localhost_address(27017 + i as u16))
                    .collect();
                client_options.repl_set_name = Some(set_name.into());
            }
            Topology::Sharded {
                shard_db_paths,
                config_db_path,
            } => {
                let config_db_port = self.next_port();
                let config_db_name = "phil-config-server";
                self.add_config_db(config_db_port, config_db_name, config_db_path.clone())?;

                let mongos_port1 = self.next_port();
                let mongos_port2 = self.next_port();

                self.add_mongos(mongos_port1, config_db_port, config_db_name)?;
                self.add_mongos(mongos_port2, config_db_port, config_db_name)?;

                for shard_db_path_set in shard_db_paths {
                    if shard_db_path_set.len() == 1 {
                        let port = self.next_port();

                        self.add_singleton_shard(port, mongos_port1, shard_db_path_set[0].clone())?;
                    } else {
                        self.add_replset_shard(mongos_port1, shard_db_path_set.to_vec())?;
                    }
                }

                client_options.hosts = vec![
                    localhost_address(mongos_port1),
                    localhost_address(mongos_port2),
                ];
            }
        };

        let cluster = Cluster {
            monger: self.monger,
            client: Client::with_options(client_options.clone())?,
            client_options: client_options,
            topology: self.topology,
            tls: self.tls,
            auth: self.credential,
            nodes: self.nodes,
        };

        Ok(cluster)
    }
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
