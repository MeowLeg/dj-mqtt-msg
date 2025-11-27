use super::*;
use serde::Deserialize;
use sqlx::{Connection, FromRow, Sqlite, SqliteConnection};
use std::collections::HashMap;
use std::process::Command;
use sysinfo::System;
use thiserror::Error;
use tokio::time::sleep;

// 自定义错误类型（整合 MQTT 错误和 JSON 解析错误）
#[derive(Error, Debug)]
pub enum MqttJsonError {
    #[error("MQTT 客户端错误: {0}")]
    MqttError(#[from] rumqttc::Error),
    #[error("JSON 解析错误: {0}")]
    JsonParseError(#[from] serde_json::Error),
    // #[error("订阅主题失败: {0}")]
    // SubscribeFailed(String),
    // #[error("连接超时（{0}秒）")]
    // ConnectTimeout(u64),
    #[error("重连次数超限（最大{0}次）")]
    ReconnectLimitExceeded(usize),
}

// 定义 JSON 消息的强类型结构（根据实际 MQTT 消息格式修改）
#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct DJData {
    #[serde(rename = "OrganizationUUID")]
    pub organization_uuid: String,
    pub biz_code: String,
    pub version: String,
    pub data: HostValue,
    pub timestamp: u128,
}

#[derive(Debug, Deserialize)]
pub struct HostValue {
    pub host: Value,
    pub sn: String,
}

#[derive(Debug, Default, FromRow)]
struct DronInfo {
    uuid: String,
    project_uuid: String,
    organization_uuid: String,
}

/// 异步 MQTT JSON 客户端（支持自动重连、JSON 解析）
pub struct AsyncMqttJsonClient {
    broker_url: String,          // MQTT  broker 地址（如 "tcp://localhost:1883"）
    client_id: String,           // 客户端 ID
    username: Option<String>,    // 用户名（可选）
    password: Option<String>,    // 密码（可选）
    topics: Vec<String>,         // 要订阅的主题列表
    qos: QoS,                    // 订阅 QoS 等级（0/1/2）
    reconnect_interval_sec: u64, // 重连间隔（秒）
    max_reconnect: usize,        // 最大重连次数
    connect_timeout_sec: u64,    // 连接超时（秒）
    // 无人机类型号
    drone_type_no: String,
    // RTMP URL 格式
    rtmp_url_format: String,
    // 数据库
    db_path: String,
    // 判断是否启动抽帧的条件
    airport_feature: String,
    drone_fly_mode_code: Vec<u8>,
    drone_land_mode_code: Vec<u8>,
    conditions: Vec<Condition>,
    fly_status: HashMap<String, bool>,
}

impl AsyncMqttJsonClient {
    /// 创建客户端实例
    pub fn new(client_id: &str, qos: QoS, cfg: &Config) -> Self {
        Self {
            broker_url: cfg.url.clone(),
            client_id: client_id.to_string(),
            username: Some(cfg.username.clone()),
            password: Some(cfg.password.clone()),
            topics: vec![cfg.topic.clone()],
            qos,
            reconnect_interval_sec: cfg.reconnect_interval_sec,
            max_reconnect: cfg.max_reconnect,
            connect_timeout_sec: cfg.connect_timeout_sec,
            drone_type_no: cfg.drone_type_no.clone(),
            rtmp_url_format: cfg.rtmp_url_format.clone(),
            db_path: cfg.db_path.clone(),
            airport_feature: cfg.airport_feature.clone(),
            drone_fly_mode_code: cfg.drone_fly_mode_code.clone(),
            drone_land_mode_code: cfg.drone_land_mode_code.clone(),
            conditions: cfg.conditions.clone(),
            fly_status: HashMap::new(),
        }
    }

    /// 设置 MQTT 用户名密码（可选）
    pub fn with_auth(mut self, username: &str, password: &str) -> Self {
        self.username = Some(username.to_string());
        self.password = Some(password.to_string());
        self
    }

    /// 设置重连配置（可选）
    pub fn with_reconnect_config(
        mut self,
        reconnect_interval_sec: u64,
        max_reconnect: usize,
        connect_timeout_sec: u64,
    ) -> Self {
        self.reconnect_interval_sec = reconnect_interval_sec;
        self.max_reconnect = max_reconnect;
        self.connect_timeout_sec = connect_timeout_sec;
        self
    }

    /// 构建 MQTT 连接选项
    fn build_mqtt_options(&self) -> MqttOptions {
        let mut mqtt_options = MqttOptions::new(
            &self.client_id,
            self.broker_url.split(':').next().unwrap_or("localhost"),
            self.broker_url
                .split(':')
                .nth(2)
                .unwrap_or("1883")
                .parse()
                .unwrap_or(1883),
        );

        // 设置用户名密码（如果有）
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            mqtt_options.set_credentials(username, password);
        }

        mqtt_options
    }

    /// 订阅主题
    async fn subscribe_topics(&self, client: &AsyncClient) -> Result<(), ClientError> {
        let subscribe_filters: Vec<SubscribeFilter> = self
            .topics
            .iter()
            .map(|topic| SubscribeFilter::new(topic.clone(), self.qos))
            .collect();

        client.subscribe_many(subscribe_filters).await?;
        println!("成功订阅主题：{:?}", self.topics);
        Ok(())
    }

    /// 解析 MQTT 消息为 JSON 结构体
    fn parse_json_message<T: for<'de> Deserialize<'de>>(
        payload: &[u8],
    ) -> Result<T, MqttJsonError> {
        serde_json::from_slice(payload).map_err(MqttJsonError::JsonParseError)
    }

    fn is_airport(&self, sn: &str) -> bool {
        sn.starts_with(&self.airport_feature)
    }

    // 判断mqtt消息是否可以出发抽帧
    fn condition(&self, host: &Value) -> Result<bool> {
        let pitch = match host.get(&self.drone_type_no) {
            Some(Value::Object(o)) => match o.get("gimbal_pitch") {
                Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                _ => 0.0,
            },
            _ => 0.0,
        }
        .abs();
        let height = match host.get("height") {
            Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => 0.0,
        }
        .abs();
        println!("pitch: {}, height: {}", pitch, height);
        // println!("---------------------------");
        Ok(self
            .conditions
            .iter()
            .any(|c| c.pitch <= pitch && c.height >= height))
    }

    /// 处理接收到的 MQTT JSON 消息
    async fn handle_message(&mut self, topic: &str, payload: &[u8]) -> Result<()> {
        // 机场与飞机的sn编号规则存在差异：8UUXN3U00A043Z 1581F8HGD25110010060
        // 同时存在多个飞机的数据，应该区分处理
        // 需要存储不同飞机的状态，用来停止操作
        match Self::parse_json_message::<DJData>(payload) {
            Ok(data) => {
                if self.is_airport(&data.data.sn) {
                    return Ok(());
                }

                // 准备启动 ffmpeg 抽帧
                println!("sn: {}", data.data.sn);
                if let Ok(b) = self.condition(&data.data.host) {
                    println!("---------------------------");
                    println!("> sn {} in action", &data.data.sn);
                    // println!("data is {:?}", &data);
                    let mut fly_status = self.fly_status.clone();
                    let old_status = *fly_status.get(&data.data.sn).unwrap_or(&false);
                    let mode_code: u8 = match data.data.host.get("mode_code") {
                        Some(Value::Number(num)) => num.as_u64().unwrap_or(0) as u8,
                        _ => 0,
                    };
                    println!(
                        "> sn {} mode_code: {}, old_status: {}",
                        &data.data.sn, mode_code, old_status
                    );
                    match mode_code {
                        // 条件太脆弱
                        m if self.drone_fly_mode_code.iter().any(|&x| x == m) => {
                            if !old_status && b {
                                // 这里必须信任流服务的存在
                                // 开始 ffmpeg 抽帧
                                println!("> sn: {}, start ffmpeg", &data.data.sn);
                                if let Ok(_) = self.start_ffmpeg(&data.data.sn).await {
                                    fly_status.insert(data.data.sn, true);
                                }
                            } else if old_status && !b {
                                // 停止 ffmpeg 抽帧
                                println!("> sn: {}, end ffmpeg", &data.data.sn);
                                if let Ok(_) = self.stop_ffmpeg(&data.data.sn) {
                                    fly_status.insert(data.data.sn, false);
                                }
                            }
                        }
                        n if self.drone_land_mode_code.iter().any(|&x| x == n) => {
                            if old_status && !b {
                                // 停止 ffmpeg 抽帧
                                println!("> sn: {}, end ffmpeg", &data.data.sn);
                                if let Ok(_) = self.stop_ffmpeg(&data.data.sn) {
                                    fly_status.insert(data.data.sn, false);
                                }
                            }
                        }
                        _ => {
                            // 不确定是否需要这么做
                            // fly_status.insert(data.data.sn, false);
                        }
                    }
                    self.fly_status = fly_status;
                }
            }
            Err(e) => {
                eprintln!(
                    "JSON 解析失败（主题: {}）: {:?}, 原始消息: {}",
                    topic,
                    e,
                    String::from_utf8_lossy(payload)
                );
            }
        }
        Ok(())
    }

    fn __20minu_before(&self) -> Result<String> {
        let now = SystemTime::now();
        let t = now.duration_since(UNIX_EPOCH)?.as_secs() - (20 * 60);
        Ok(format!("{t}"))
    }

    async fn start_ffmpeg(&self, sn: &str) -> Result<()> {
        // 启动 MQTT 客户端
        let mut conn = SqliteConnection::connect(&self.db_path).await?;
        let sql = "SELECT uuid, project_uuid, organization_uuid FROM sn where sn = ? and created_timestamp > ?";
        // 如没有收到启动信号，也强制启动，用空值
        let row = match sqlx::query_as::<Sqlite, DronInfo>(sql)
            .bind(sn)
            .bind(self.__20minu_before()?)
            .fetch_one(&mut conn)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                println!("> Err: {}", e);
                DronInfo::default()
            }
        };
        let mut url = self.rtmp_url_format.clone();
        url.push_str(sn);
        println!("> url: {}", &url);
        if !self.__exists_ffmpeg(sn)? {
            match Command::new("./ffmpeg-dump-jpeg.exe")
                .arg("--config")
                .arg("./dump_config.toml")
                .arg("--url")
                .arg(url)
                .arg("--uuid")
                .arg(&row.uuid)
                .arg("--project-uuid")
                .arg(&row.project_uuid)
                .arg("--organization-uuid")
                .arg(&row.organization_uuid)
                .spawn()
            {
                Ok(_) => println!("ffmpeg-dump-jpeg started"),
                Err(e) => println!("ffmpeg-dump-jpeg failed to start: {}", e),
            }
        }

        Ok(())
    }

    fn __exists_ffmpeg(&self, sn: &str) -> Result<bool> {
        let sys = System::new_all();
        for p in sys.processes().values() {
            if let Some(s) = p.name().to_str()
                && s.contains("ffmpeg-dump-jpeg")
                && p.cmd()
                    .iter()
                    .any(|arg| arg.to_str().unwrap_or("").contains(sn))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn stop_ffmpeg(&self, sn: &str) -> Result<()> {
        // 停止 MQTT 客户端
        let sys = System::new_all();
        for p in sys.processes().values() {
            if let Some(s) = p.name().to_str()
                && s.contains("ffmpeg-dump-jpeg")
                && p.cmd()
                    .iter()
                    .any(|arg| arg.to_str().unwrap_or("").contains(sn))
            {
                let _ = p.kill();
                return Ok(());
            }
        }

        Ok(())
    }

    /// 启动 MQTT 客户端（异步阻塞，持续接收消息）
    pub async fn start(&mut self) -> Result<(), MqttJsonError> {
        let mut reconnect_count = 0;

        loop {
            // 构建 MQTT 选项并创建客户端
            let mqtt_options = self.build_mqtt_options();
            let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10); // 10 是消息队列大小

            println!("正在连接 MQTT Broker: {}", self.broker_url);
            match self.subscribe_topics(&client).await {
                Ok(_) => {
                    // 订阅成功，开始监听消息
                    println!("MQTT 连接成功，等待接收消息...");
                    reconnect_count = 0; // 重置重连计数

                    // 循环处理 MQTT 事件
                    while let Ok(event) = event_loop.poll().await {
                        match event {
                            // 接收发布消息
                            // Incoming(Publish(publish)) => {
                            Event::Incoming(Incoming::Publish(publish)) => {
                                let topic = publish.topic;
                                let payload = publish.payload;
                                // 处理消息（异步，不阻塞事件循环）
                                match self.handle_message(&topic, &payload).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        println!("处理消息失败: {e:?}");
                                    }
                                }
                            }
                            // 连接断开（触发重连）
                            Event::Incoming(Incoming::Disconnect) => {
                                eprintln!("MQTT 连接断开，准备重连...");
                                break;
                            }
                            // 其他事件（如重连成功、订阅确认等）
                            _ => continue,
                        }
                    }
                }
                Err(e) => {
                    // 订阅失败，触发重连
                    eprintln!("订阅主题失败: {e:?}");
                    reconnect_count += 1;
                }
            }

            // 重连次数超限
            if reconnect_count > self.max_reconnect {
                return Err(MqttJsonError::ReconnectLimitExceeded(self.max_reconnect));
            }

            // 重连间隔（异步睡眠）
            println!(
                "第 {} 次重连（间隔 {} 秒）...",
                reconnect_count, self.reconnect_interval_sec
            );
            sleep(Duration::from_secs(self.reconnect_interval_sec)).await;
        }
    }
}
