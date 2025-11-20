use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use ustr::Ustr;

use crate::{
    Result,
    chart::study::{IndicatorInput, InputValue},
    client::misc::get_indicator_metadata,
    models::{FinancialPeriod, UserCookies},
};
#[derive(Debug, Clone, PartialEq)]
pub enum BuiltinIndicators {
    All,
    Fundamental,
    Standard,
    Candlestick,
}

impl std::fmt::Display for BuiltinIndicators {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BuiltinIndicators::All => write!(f, "all"),
            BuiltinIndicators::Fundamental => write!(f, "fundamental"),
            BuiltinIndicators::Standard => write!(f, "standard"),
            BuiltinIndicators::Candlestick => write!(f, "candlestick"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PineInfo {
    pub user_id: i64,
    pub script_name: String,
    pub script_source: String,
    #[serde(rename(deserialize = "scriptIdPart"))]
    pub script_id: String,
    pub script_access: String,
    #[serde(rename(deserialize = "version"))]
    pub script_version: String,
    pub extra: PineInfoExtra,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PineInfoExtra {
    pub financial_period: Option<FinancialPeriod>,
    pub fund_id: Option<String>,
    pub fundamental_category: Option<String>,
    pub is_auto: bool,
    pub is_beta: bool,
    pub is_built_in: bool,
    pub is_candle_stick: bool,
    pub is_fundamental_study: bool,
    pub is_hidden_study: bool,
    pub is_mtf_resolution: bool,
    pub is_new: bool,
    pub is_pine_editor_new_template: bool,
    pub is_updated: bool,
    pub kind: String,
    pub short_description: String,
    pub source_inputs_count: i64,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TranslateResponse {
    pub success: bool,
    pub result: PineMetadata,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchResponse {
    pub next: String,
    pub results: Vec<PineSearchResult>,
}

#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PineSearchResult {
    pub image_url: String,
    pub script_name: String,
    pub script_source: String,
    pub access: i64,
    pub script_id_part: String,
    pub version: String,
    pub extra: PineSearchExtra,
    pub agree_count: i64,
    pub author: PineSearchAuthor,
    pub weight: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PineSearchExtra {
    pub kind: Option<String>,
    pub source_inputs_count: Option<i64>,
    #[serde(rename = "isMTFResolution")]
    pub is_mtf_resolution: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PineSearchAuthor {
    pub id: i64,
    pub username: Ustr,
    #[serde(rename = "is_broker")]
    pub is_broker: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
pub struct PineMetadata {
    #[serde(rename(deserialize = "IL"))]
    pub il: Ustr,
    #[serde(rename(deserialize = "ilTemplate"))]
    pub il_template: Ustr,
    #[serde(rename(deserialize = "metaInfo"))]
    pub data: PineMetadataInfo,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct PineMetadataInfo {
    pub id: Ustr,
    #[serde(rename(deserialize = "scriptIdPart"))]
    pub script_id: Ustr,
    pub description: Ustr,
    pub short_description: Ustr,
    pub financial_period: Option<FinancialPeriod>,
    pub grouping_key: Ustr,
    pub is_fundamental_study: bool,
    pub is_hidden_study: bool,
    pub is_tv_script: bool,
    pub is_tv_script_stub: bool,
    pub is_price_study: bool,
    pub inputs: Vec<PineInput>,
    pub defaults: HashMap<String, Value>,
    pub palettes: HashMap<String, Value>,
    pub pine: HashMap<String, String>,
    pub plots: Vec<Plot>,
    pub styles: HashMap<String, Value>,
    pub uses_private_lib: bool,
    pub warnings: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct Plot {
    pub id: String,
    pub plot_type: String,
    pub target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct PineInput {
    pub name: String,
    pub inline: String,
    pub id: String,
    pub defval: Value,
    pub is_hidden: bool,
    pub is_fake: bool,
    pub optional: bool,
    pub options: Vec<String>,
    pub tooltip: Option<String>,
    #[serde(rename(deserialize = "type"))]
    pub input_type: String,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize, Serialize, Copy)]
pub enum ScriptType {
    #[default]
    Script,
    IntervalScript,
    StrategyScript,
    VolumeBasicStudies,
    FixedBasicStudies,
    FixedVolumeByPrice,
    SessionVolumeByPrice,
    SessionRoughVolumeByPrice,
    SessionDetailedVolumeByPrice,
    VisibleVolumeByPrice,
}

impl std::fmt::Display for ScriptType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ScriptType::Script => write!(f, "Script@tv-scripting-101!"),
            ScriptType::IntervalScript => write!(f, "Internal@tv-scripting-101!"),
            ScriptType::StrategyScript => write!(f, "StrategyScript@tv-scripting-101!"),
            ScriptType::VolumeBasicStudies => write!(f, "Volume@tv-basicstudies-144"),
            ScriptType::FixedBasicStudies => write!(f, "VbPFixed@tv-basicstudies-139!"),
            ScriptType::FixedVolumeByPrice => write!(f, "VbPFixed@tv-volumebyprice-53!"),
            ScriptType::SessionVolumeByPrice => write!(f, "VbPSessions@tv-volumebyprice-53"),
            ScriptType::SessionRoughVolumeByPrice => {
                write!(f, "VbPSessionsRough@tv-volumebyprice-53!")
            }
            ScriptType::SessionDetailedVolumeByPrice => {
                write!(f, "VbPSessionsDetailed@tv-volumebyprice-53!")
            }
            ScriptType::VisibleVolumeByPrice => write!(f, "VbPVisible@tv-volumebyprice-53"),
        }
    }
}

impl From<String> for ScriptType {
    fn from(_value: String) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct PineIndicator {
    pub script_id: Ustr,
    pub script_version: Ustr,
    pub script_type: ScriptType,
    pub metadata: PineMetadata,
}

pub struct PineIndicatorBuilder {
    user: Option<UserCookies>,
}

impl PineIndicatorBuilder {
    pub fn user(&mut self, user: UserCookies) -> &mut Self {
        self.user = Some(user);
        self
    }

    pub async fn fetch(
        &mut self,
        script_id: &str,
        script_version: &str,
        script_type: ScriptType,
    ) -> Result<PineIndicator> {
        let metadata = match &self.user {
            Some(user) => get_indicator_metadata(Some(user), script_id, script_version).await?,
            None => get_indicator_metadata(None, script_id, script_version).await?,
        };
        Ok(PineIndicator {
            script_id: Ustr::from(script_id),
            script_version: Ustr::from(script_version),
            script_type,
            metadata,
        })
    }
}
fn parse_rgba(rgba_str: &str) -> Option<u32> {
    // Убираем "rgba(" и ")"
    let cleaned = rgba_str.trim_start_matches("rgba(").trim_end_matches(')');

    // Разбиваем на части
    let parts: Vec<&str> = cleaned.split(',').collect();

    if parts.len() == 4 {
        let r = parts[0].trim().parse::<u8>().ok()?;
        let g = parts[1].trim().parse::<u8>().ok()?;
        let b = parts[2].trim().parse::<u8>().ok()?;
        let a = parts[3].trim().parse::<f32>().ok()?;

        Some(rgba_to_decimal(r, g, b, a))
    } else {
        None
    }
}

fn rgba_to_decimal(r: u8, g: u8, b: u8, a: f32) -> u32 {
    let alpha_byte = (a.clamp(0.0, 1.0) * 255.0) as u32;
    (alpha_byte << 24) | ((r as u32) << 16) | ((g as u32) << 8) | (b as u32)
}

impl PineIndicator {
    pub fn build() -> PineIndicatorBuilder {
        PineIndicatorBuilder { user: None }
    }

    pub fn to_study_inputs(&self) -> Result<Value> {
        let mut inputs: HashMap<Ustr, IndicatorInput> = HashMap::new();
        inputs.insert(
            Ustr::from("text"),
            IndicatorInput::String(Ustr::from(&self.metadata.il_template)),
        );
        inputs.insert(
            Ustr::from("pineId"),
            IndicatorInput::String(Ustr::from(&self.script_id)),
        );
        inputs.insert(
            Ustr::from("pineVersion"),
            IndicatorInput::String(Ustr::from(&self.script_version)),
        );
        self.metadata.data.inputs.iter().for_each(|input| {
            if input.id == "text" || input.id == "pineId" || input.id == "pineVersion" {
                return;
            }
            let mut defval = None;
            if input.input_type == "color" {
                let value = input.defval.clone().to_string();
                let cleaned_value = value.trim_matches('"');
                if cleaned_value.starts_with("#") {
                    let hex_str = &cleaned_value[1..];
                    let hex_decimal = u32::from_str_radix(&format!("FF{}", hex_str), 16).unwrap();
                    defval = Some(Value::from(hex_decimal));
                } else {
                    defval = Some(Value::from(parse_rgba(cleaned_value).unwrap()));
                };
            } else {
                defval = Some(input.defval.clone())
            };
            inputs.insert(
                Ustr::from(&input.id),
                IndicatorInput::IndicatorInput(InputValue {
                    v: defval.unwrap().clone(),
                    f: Value::from(input.is_fake),
                    t: Value::from(input.input_type.clone()),
                }),
            );
        });

        let json_value = serde_json::to_value(inputs)?;
        Ok(json_value)
    }
}
