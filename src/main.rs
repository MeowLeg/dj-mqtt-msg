use anyhow::Result as AnyResult;
use anyhow::Result;
use rumqttc::{AsyncClient, ClientError, Event, Incoming, MqttOptions, QoS, SubscribeFilter};
use serde::Deserialize;
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs::File, io::Read};

mod client;
use client::*;

#[derive(Debug, Deserialize)]
struct Config {
    url: String,
    topic: String,
    username: String,
    password: String,
    reconnect_interval_sec: u64,
    max_reconnect: usize,
    connect_timeout_sec: u64,
    drone_type_no: String,
    rtmp_url_format: String,
    db_path: String,
    conditions: Vec<Condition>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
struct Condition {
    pitch: i32,
    height: i32,
}

fn get_current_timestamp_str() -> AnyResult<String> {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH)?;
    let timestamp = since_epoch.as_millis();
    Ok(timestamp.to_string())
}

fn read_from_toml(f: &str) -> Result<Config> {
    let mut file = File::open(f)?;
    let mut s = String::new();
    file.read_to_string(&mut s)?;
    let config: Config = toml::from_str(&s)?;
    Ok(config)
}

// 示例：启动异步 MQTT 客户端接收 JSON 消息
#[tokio::main]
async fn main() -> AnyResult<()> {
    // 初始化日志（可选，用于调试）
    env_logger::init();

    // 读取配置文件
    let config = read_from_toml("config.toml")?;

    // 1. 配置 MQTT 客户端参数
    let client_id = format!("zscm_kjb_{}", get_current_timestamp_str()?); // 客户端 ID（唯一标识）
    let qos = QoS::AtMostOnce; // QoS 等级（AtMostOnce=0, AtLeastOnce=1, ExactlyOnce=2）

    // 2. 创建客户端并配置（可选：添加用户名密码、重连参数）
    let mut cli = AsyncMqttJsonClient::new(&client_id, qos, &config)
        .with_auth(&config.username, &config.password) // 可选：设置 MQTT 用户名密码
        .with_reconnect_config(
            config.reconnect_interval_sec,
            config.max_reconnect,
            config.connect_timeout_sec,
        ); // 重连间隔 5 秒，最大 10 次，连接超时 10 秒

    // 3. 启动客户端（异步阻塞，持续接收消息）
    cli.start().await?;

    Ok(())
}
