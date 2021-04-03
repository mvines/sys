use {reqwest::Client, serde_json::json, std::env};

pub struct Notifier {
    client: Client,
    slack_webhook: Option<String>,
}

impl Notifier {
    pub fn default() -> Self {
        let slack_webhook = env::var("SLACK_WEBHOOK").ok();
        Notifier {
            client: Client::new(),
            slack_webhook,
        }
    }

    pub async fn send(&self, msg: &str) {
        if let Some(ref slack_webhook) = self.slack_webhook {
            let data = json!({ "text": msg });

            if let Err(err) = self.client.post(slack_webhook).json(&data).send().await {
                eprintln!("Failed to send Slack message: {:?}", err);
            }
        }
    }
}
