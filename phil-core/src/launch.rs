use std::{
    ffi::OsString,
    path::PathBuf,
    process::{Child, Command},
    time::Duration,
};

use monger_core::Monger;
use mongodb::{
    bson::{doc, Bson},
    options::{ClientOptions, StreamAddress},
    sync::Client,
};
use serde::Deserialize;

use crate::{
    cluster::{Cluster, Credential, TlsOptions, Topology},
    error::Result,
};

fn localhost_address(port: u16) -> StreamAddress {
    StreamAddress {
        hostname: "localhost".into(),
        port: Some(port),
    }
}

#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) process: Child,
    pub(crate) options: MongodOptions,
}

#[derive(Debug)]
pub(crate) struct MongodOptions {
    port: u16,
    db_path: Option<PathBuf>,
    config_server: bool,
    shard_server: bool,
    repl_set_name: Option<String>,
}

#[derive(Debug)]
pub(crate) struct Router {
    process: Child,
    options: MongosOptions,
}

#[derive(Debug)]
pub(crate) struct MongosOptions {
    port: u16,
    config_db_port: u16,
    config_db_name: String,
}

#[derive(Debug)]
pub(crate) struct Launcher {
    monger: Monger,
    topology: Topology,
    version: String,
    tls: Option<TlsOptions>,
    credential: Option<Credential>,
    nodes: Vec<Node>,
    routers: Vec<Router>,
    next_port: u16,
    shard_count: u8,
    verbose: bool,
    deprecated_tls_options: bool,
    extra_mongod_args: Vec<OsString>,
}

impl Launcher {
    pub(crate) fn new(
        topology: Topology,
        version: String,
        tls: Option<TlsOptions>,
        credential: Option<Credential>,
        verbose: bool,
        deprecated_tls_options: bool,
        extra_mongod_args: Vec<OsString>,
    ) -> Result<Self> {
        Ok(Self {
            monger: Monger::new()?,
            topology,
            version,
            tls,
            credential,
            nodes: Default::default(),
            routers: Default::default(),
            next_port: 27017,
            shard_count: 0,
            verbose,
            deprecated_tls_options,
            extra_mongod_args,
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

    fn repl_set_addresses(&self, repl_set_name: String) -> impl Iterator<Item = u16> + '_ {
        self.nodes.iter().filter_map(move |node| {
            if node.options.repl_set_name == Some(repl_set_name.clone()) {
                Some(node.options.port)
            } else {
                None
            }
        })
    }

    fn start_mongod(&mut self, options: MongodOptions) -> Result<Node> {
        let mut args: Vec<OsString> = vec!["--port".into(), options.port.to_string().into()];

        if let Some(ref path) = options.db_path {
            args.push("--dbpath".into());
            args.push(path.clone().into());
        }

        if let Some(ref credential) = self.credential {
            args.extend_from_slice(&[
                "--auth".into(),
                "--keyFile".into(),
                credential.key_file.as_os_str().into(),
            ]);
        }

        if let Some(ref set_name) = options.repl_set_name {
            args.extend_from_slice(&["--replSet".into(), set_name.into()]);
        }

        if options.config_server {
            args.push("--configsvr".into());
        }

        if let Some(ref tls_options) = self.tls {
            if self.deprecated_tls_options {
                args.extend_from_slice(&[
                    "--sslMode".into(),
                    "requireSSL".into(),
                    "--sslCAFile".into(),
                    tls_options.ca_file_path.clone().into(),
                    "--sslPEMKeyFile".into(),
                    tls_options.server_cert_file_path.clone().into(),
                ]);
            } else {
                args.extend_from_slice(&[
                    "--tlsMode".into(),
                    "requireTLS".into(),
                    "--tlsCAFile".into(),
                    tls_options.ca_file_path.clone().into(),
                    "--tlsCertificateKeyFile".into(),
                    tls_options.server_cert_file_path.clone().into(),
                ]);
            }

            if tls_options.weak_tls {
                args.push("--tlsAllowConnectionsWithoutCertificates".into());
            }
        }

        if options.shard_server {
            args.push("--shardsvr".into());
        }

        if !self.extra_mongod_args.is_empty() {
            args.extend_from_slice(&self.extra_mongod_args);
        }

        if self.verbose {
            print!("    starting");

            if options.config_server {
                print!(" config server");
            }

            if options.shard_server {
                print!(" shard server");
            }

            print!(" mongod on port {}", options.port);

            if let Some(ref name) = options.repl_set_name {
                print!(" in repl set '{}'", name);
            }

            if self.credential.is_some() && self.tls.is_some() {
                print!(" with auth and TLS enabled");
            } else if self.credential.is_some() {
                print!(" with auth enabled");
            } else if self.tls.is_some() {
                print!(" with TLS enabled");
            }

            println!("...");
        }

        let process = self.monger.start_mongod(args, &self.version, false)?;
        let node = Node { process, options };

        Ok(node)
    }

    fn configure_repl_set(&self, set_name: &str, config_server: bool, log: bool) -> Result<()> {
        let nodes: Vec<_> = self
            .repl_set_addresses(set_name.into())
            .enumerate()
            .map(|(i, port)| {
                Bson::Document(doc! {
                    "_id": i as i32,
                    "host": localhost_address(port).to_string(),
                })
            })
            .collect();

        let config = doc! {
            "_id": set_name,
            "configsvr": config_server,
            "members": nodes
        };

        let options = ClientOptions::builder()
            .hosts(vec![localhost_address(
                self.repl_set_addresses(set_name.into()).next().unwrap(),
            )])
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

        if log {
            println!("configuring replica set...");
        } else if self.verbose {
            println!("    configuring replica set...");
        }

        loop {
            let response = db.run_command(cmd.clone(), None);

            let response = match response {
                Ok(response) => response,
                Err(..) => {
                    std::thread::sleep(Duration::from_millis(250));

                    continue;
                }
            };

            let CommandResponse { ok, code_name } = mongodb::bson::from_document(response.clone())?;

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

        if log {
            println!("waiting for primary to be elected...");
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

            let ReplSetStatus { members } = mongodb::bson::from_document(response)?;

            if members.iter().any(|member| member.state_str == "PRIMARY") {
                return Ok(());
            }

            std::thread::sleep(Duration::from_millis(250));
        }
    }

    fn start_repl_set(
        &mut self,
        repl_set_name: &str,
        config_server: bool,
        shard_server: bool,
        db_paths: Vec<PathBuf>,
        log: bool,
    ) -> Result<()> {
        if log {
            println!("starting replica set servers...");
        }

        for db_path in db_paths {
            let options = MongodOptions {
                port: self.next_port(),
                db_path: Some(db_path),
                config_server,
                shard_server,
                repl_set_name: Some(repl_set_name.into()),
            };

            let node = self.start_mongod(options)?;

            self.nodes.push(node);
        }

        self.configure_repl_set(repl_set_name, config_server, log)?;

        Ok(())
    }

    fn add_config_db(&mut self, port: u16, name: &str, db_path: PathBuf) -> Result<()> {
        let config_db_options = MongodOptions {
            port,
            db_path: Some(db_path),
            config_server: true,
            shard_server: false,
            repl_set_name: Some(name.into()),
        };

        let node = self.start_mongod(config_db_options)?;
        self.nodes.push(node);

        self.configure_repl_set(name, true, false)?;

        Ok(())
    }

    fn start_mongos(&self, options: MongosOptions) -> Result<Router> {
        let mut args: Vec<OsString> = vec![
            "--port".into(),
            options.port.to_string().into(),
            "--configdb".into(),
            format!(
                "{}/localhost:{}",
                options.config_db_name, options.config_db_port
            )
            .into(),
        ];

        let mut potential_set_parameter_args = self.extra_mongod_args.clone();

        if let Some(default_args) = self.monger.get_default_args()? {
            potential_set_parameter_args
                .extend(default_args.split_whitespace().map(OsString::from));
        }

        dbg!(&potential_set_parameter_args);

        if let Some(set_param_index) = potential_set_parameter_args
            .iter()
            .position(|arg| arg == "--setParameter")
        {
            args.push("--setParameter".into());
            args.extend(
                potential_set_parameter_args
                    .get(set_param_index + 1)
                    .cloned(),
            );
        }

        if let Some(ref tls_options) = self.tls {
            if self.deprecated_tls_options {
                args.extend_from_slice(&[
                    "--sslMode".into(),
                    "requireSSL".into(),
                    "--sslCAFile".into(),
                    tls_options.ca_file_path.clone().into(),
                    "--sslPEMKeyFile".into(),
                    tls_options.server_cert_file_path.clone().into(),
                    "--sslAllowInvalidCertificates".into(),
                ]);
            } else {
                args.extend_from_slice(&[
                    "--tlsMode".into(),
                    "requireTLS".into(),
                    "--tlsCAFile".into(),
                    tls_options.ca_file_path.clone().into(),
                    "--tlsCertificateKeyFile".into(),
                    tls_options.server_cert_file_path.clone().into(),
                    "--tlsAllowInvalidCertificates".into(),
                ]);
            }

            if tls_options.weak_tls {
                args.push("--tlsAllowConnectionsWithoutCertificates".into());
            }
        }

        if let Some(ref credential) = self.credential {
            args.extend_from_slice(&["--keyFile".into(), credential.key_file.as_os_str().into()]);
        }

        if self.verbose {
            print!("starting mongos sharding router on port {}", options.port);

            if self.credential.is_some() && self.tls.is_some() {
                print!(" with auth and TLS enabled");
            } else if self.credential.is_some() {
                print!(" with auth enabled");
            } else if self.tls.is_some() {
                print!(" with TLS enabled");
            }

            println!("...");
        }

        let process = self
            .monger
            .run_background_command("mongos", args, &self.version)?;
        let router = Router { process, options };

        Ok(router)
    }

    fn add_singleton_shard(&mut self, port: u16, mongos_port: u16, db_path: PathBuf) -> Result<()> {
        let options = MongodOptions {
            port,
            db_path: Some(db_path),
            config_server: false,
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

        if self.verbose {
            println!("    adding single shard on port {} to cluster...", port);
        }

        let db = client.database("admin");
        let cmd = doc! {
            "addShard": localhost_address(port).to_string(),
            "name": name
        };

        loop {
            let response = db.run_command(cmd.clone(), None);

            let response = match response {
                Ok(response) => response,
                Err(..) => {
                    std::thread::sleep(Duration::from_millis(250));

                    continue;
                }
            };

            let CommandResponse { ok, .. } = mongodb::bson::from_document(response.clone())?;

            if ok == 1.0 {
                break;
            }
        }

        Ok(())
    }

    fn add_replset_shard(&mut self, mongos_port: u16, db_paths: Vec<PathBuf>) -> Result<()> {
        let name = format!("phil-replset-shard-{}", self.next_shard_id());
        self.start_repl_set(&name, false, true, db_paths, false)?;

        let options = ClientOptions::builder()
            .hosts(vec![localhost_address(mongos_port)])
            .credential(self.credential.clone().map(Into::into))
            .tls(self.tls.clone().map(Into::into))
            .build();

        let client = Client::with_options(options)?;

        let node_addresses: Vec<_> = self
            .repl_set_addresses(name.clone())
            .map(|port| localhost_address(port).to_string())
            .collect();

        if self.verbose {
            println!(
                "    adding replica set shard with set name {} to cluster...",
                name
            );
        }

        let db = client.database("admin");
        let cmd = doc! {
            "addShard": format!("{}/{}", name, node_addresses.join(",")),
            "name": name
        };

        loop {
            let response = db.run_command(cmd.clone(), None);

            let response = match response {
                Ok(response) => response,
                Err(..) => {
                    std::thread::sleep(Duration::from_millis(250));

                    continue;
                }
            };

            let CommandResponse { ok, .. } = mongodb::bson::from_document(response.clone())?;

            if ok == 1.0 {
                break;
            }
        }

        Ok(())
    }

    pub(crate) fn initialize_cluster(mut self) -> Result<Cluster> {
        let mut client_options = ClientOptions::builder()
            .tls(self.tls.clone().map(Into::into))
            .build();
        let credential = self.credential.take();

        match self.topology.clone() {
            Topology::Single => {
                let options = MongodOptions {
                    port: 27017,
                    db_path: None,
                    config_server: false,
                    shard_server: false,
                    repl_set_name: None,
                };

                println!("starting single server...");

                let node = self.start_mongod(options)?;
                self.nodes.push(node);

                client_options.hosts = vec![localhost_address(27017)];
            }
            Topology::ReplicaSet { set_name, db_paths } => {
                self.start_repl_set(&set_name, false, false, db_paths.to_vec(), true)?;

                client_options.hosts = (0..db_paths.len())
                    .into_iter()
                    .map(|i| localhost_address(27017 + i as u16))
                    .collect();
                client_options.repl_set_name = Some(set_name.into());
            }
            Topology::Sharded {
                num_mongos,
                shard_db_paths,
                config_db_path,
            } => {
                let mongos_ports: Vec<_> = (0..num_mongos).map(|_| self.next_port()).collect();

                println!("starting config server...");

                let config_db_port = self.next_port();
                let config_db_name = "phil-config-server";
                self.add_config_db(config_db_port, config_db_name, config_db_path.clone())?;

                println!("starting sharding routers...");

                for mongos_port in &mongos_ports {
                    let mongos_options1 = MongosOptions {
                        port: *mongos_port,
                        config_db_port,
                        config_db_name: config_db_name.into(),
                    };

                    let router = self.start_mongos(mongos_options1)?;
                    self.routers.push(router);
                }

                println!("adding shards...");

                let mut first = true;

                for shard_db_path_set in shard_db_paths {
                    if self.verbose && !first {
                        println!();
                    }

                    if shard_db_path_set.len() == 1 {
                        let port = self.next_port();

                        self.add_singleton_shard(
                            port,
                            mongos_ports[0],
                            shard_db_path_set[0].clone(),
                        )?;
                    } else {
                        self.add_replset_shard(mongos_ports[0], shard_db_path_set.to_vec())?;
                    }

                    first = false;
                }

                client_options.hosts = mongos_ports.into_iter().map(localhost_address).collect();
            }
        };

        if let Some(credential) = credential {
            self.credential = Some(credential.clone());

            println!("adding user...");

            let client = Client::with_options(client_options.clone())?;
            client.database("admin").run_command(
                doc! {
                    "createUser": credential.username.clone(),
                    "pwd": credential.password.clone(),
                    "roles": ["root"],
                },
                None,
            )?;

            client_options.credential = Some(credential.into());

            let pre_auth_nodes = std::mem::replace(&mut self.nodes, Vec::new());

            println!("restarting servers with auth enabled...");

            for mut pre_auth_node in pre_auth_nodes {
                if self.verbose {
                    println!(
                        "    shutting down mongod on port {}...",
                        pre_auth_node.options.port
                    );
                }

                Command::new("kill")
                    .args(&[pre_auth_node.process.id().to_string()])
                    .spawn()?
                    .wait()?;

                pre_auth_node.process.wait()?;

                let auth_node = self.start_mongod(pre_auth_node.options)?;
                self.nodes.push(auth_node);
            }

            let pre_auth_routers = std::mem::replace(&mut self.routers, Vec::new());

            if !pre_auth_routers.is_empty() {
                println!("restarting sharding routers with auth enabled...");
            }

            for mut pre_auth_router in pre_auth_routers {
                if self.verbose {
                    println!(
                        "    shutting down mongos on port {}...",
                        pre_auth_router.options.port
                    );
                }

                Command::new("kill")
                    .args(&[pre_auth_router.process.id().to_string()])
                    .spawn()?
                    .wait()?;

                pre_auth_router.process.wait()?;

                let auth_router = self.start_mongos(pre_auth_router.options)?;
                self.routers.push(auth_router);
            }
        }

        println!("Cluster is ready!\n");

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
