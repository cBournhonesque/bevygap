/// Plugin that fetches the context from the Edgegap API, which contains
/// information relevant to the deployment of this gameserver, such as its
/// location, public IP, and other metadata.
use crate::arbitrium_env::ArbitriumEnv;
use crate::bevy_tokio_tasks::TokioTasksRuntime;
use bevy::prelude::*;
use log::{error, info};
use serde_json::{Map, Value};

#[derive(Event)]
pub(crate) struct ContextLoaded;

#[derive(Resource, Debug, Clone)]
pub struct ArbitriumContext {
    context: serde_json::Map<String, serde_json::Value>,
}

impl ArbitriumContext {
    pub fn from_local_env(env: &ArbitriumEnv) -> Self {
        let location = serde_json::from_str::<Value>(&env.deployment_location)
            .ok()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_else(|| {
                let mut location = Map::new();
                location.insert("city".to_string(), Value::String("Local".to_string()));
                location.insert("country".to_string(), Value::String("Dev".to_string()));
                location
            });
        let ports = serde_json::from_str::<Value>(&env.ports_mapping)
            .ok()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_else(Map::new);

        let mut context = Map::new();
        context.insert("request_id".into(), Value::String(env.request_id.clone()));
        context.insert("public_ip".into(), Value::String(env.public_ip.clone()));
        context.insert("fqdn".into(), Value::String("localhost".to_string()));
        context.insert("sockets".into(), Value::Number(1.into()));
        context.insert("location".into(), Value::Object(location));
        context.insert("ports".into(), Value::Object(ports));
        Self { context }
    }

    pub fn location(&self) -> String {
        let location = self
            .context
            .get("location")
            .expect("Missing location key in context");
        let city = location.get("city").expect("Missing city key in context");
        let country = location
            .get("country")
            .expect("Missing country key in context");
        format!("{}, {}", city, country)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.context).expect("Failed to serialize context to JSON")
    }

    pub fn sockets(&self) -> u32 {
        let serde_json::Value::Number(sockets) = self
            .context
            .get("sockets")
            .expect("Missing sockets key in context")
        else {
            panic!("Sockets is not a number!");
        };
        sockets.as_u64().expect("Sockets is not a number") as u32
    }

    pub fn top_level_string(&self, key: &str) -> String {
        self.context
            .get(key)
            .unwrap_or_else(|| panic!("Missing {key} key in context"))
            .as_str()
            .unwrap_or_else(|| panic!("{key} is not a string"))
            .to_string()
    }

    pub fn request_id(&self) -> String {
        self.top_level_string("request_id")
    }

    pub fn public_ip(&self) -> String {
        self.top_level_string("public_ip")
    }

    pub fn fqdn(&self) -> String {
        self.top_level_string("fqdn")
    }

    pub fn game_external_port(&self) -> Option<u16> {
        let ports = self.context.get("ports")?.as_object()?;
        let port_info = ports.get("game").or_else(|| ports.values().next())?;
        let external = port_info.get("external")?.as_u64()?;
        u16::try_from(external).ok()
    }
}

/// Load context from the Edgegap API and insert into world resource.
async fn fetch_context_from_api(
    context_url: &str,
    context_token: &str,
) -> Result<ArbitriumContext, async_nats::Error> {
    let context_response = crate::http_client::get_context(context_url, context_token).await?;

    let serde_json::Value::Object(context_map) = context_response else {
        panic!("Context is not an object");
    };
    info!("Context fetched: {:?}", context_map);

    Ok(ArbitriumContext {
        context: context_map,
    })
}

pub fn fetch_context_on_nats_connected(
    _trigger: On<crate::plugin::NatsConnected>,
    runtime: ResMut<TokioTasksRuntime>,
    arb_env: Res<ArbitriumEnv>,
) {
    if arb_env.local_context {
        let arb_context = ArbitriumContext::from_local_env(&arb_env);
        info!("Using local mock Edgegap context: {arb_context:?}");
        runtime.spawn_background_task(|mut ctx| async move {
            ctx.run_on_main_thread(move |ctx| {
                ctx.world.insert_resource(arb_context);
                ctx.world.trigger(ContextLoaded);
            })
            .await;
        });
        return;
    }

    let context_url = arb_env.context_url.clone();
    let context_token = arb_env.context_token.clone();
    info!("Fetching context: {context_url} ::::  {context_token}");

    runtime.spawn_background_task(|mut ctx| async move {
        let arb_context = fetch_context_from_api(&context_url, &context_token)
            .await
            // .expect("Failed to fetch context from Edgegap API");
            .unwrap_or_else(|_err| {
                error!("Failed to fetch context from Edgegap API: {_err}");
                panic!("Failed to fetch context");
                // // panic, or use a fake value:
                // warn!("Using fake context!");
                // let mut context = serde_json::Map::new();
                // context.insert("fake_data".into(), "lol".into());
                // context.insert("fqdn".into(), "rj.example.com".into());
                // ArbitriumContext { context }
            });
        info!("Got Context: {arb_context:?}");
        ctx.run_on_main_thread(move |ctx| {
            ctx.world.insert_resource(arb_context);
            ctx.world.trigger(ContextLoaded);
        })
        .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_context_contains_expected_endpoint() {
        let env = ArbitriumEnv {
            request_id: "local-lightrider".to_string(),
            delete_url: "local-mock://delete".to_string(),
            delete_token: "token".to_string(),
            deployment_location: r#"{"city":"Local","country":"Dev"}"#.to_string(),
            context_url: "local-mock://context".to_string(),
            context_token: "token".to_string(),
            public_ip: "127.0.0.1".to_string(),
            ports_mapping: r#"{"game":{"internal":7777,"external":30090,"protocol":"UDP"}}"#
                .to_string(),
            local_context: true,
        };

        let context = ArbitriumContext::from_local_env(&env);
        assert_eq!(context.request_id(), "local-lightrider");
        assert_eq!(context.public_ip(), "127.0.0.1");
        assert_eq!(context.game_external_port(), Some(30090));
    }
}
