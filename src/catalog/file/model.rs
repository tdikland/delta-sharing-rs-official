use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::catalog::{Schema, Share, Table};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShareFile {
    shares: Vec<ShareConfig>,
}

impl ShareFile {
    pub fn new() -> Self {
        Self { shares: vec![] }
    }

    pub fn list_shares(&self, recipient: &str) -> Vec<Share> {
        self.shares
            .iter()
            .filter(|cfg| match &cfg.recipients {
                Some(r) => r.iter().any(|r| r == recipient),
                None => true,
            })
            .map(|cfg| cfg.to_share())
            .collect()
    }

    pub fn list_schemas(&self, recipient: &str, share_name: &str) -> Vec<Schema> {
        self.shares
            .iter()
            .filter(|share_cfg| match &share_cfg.recipients {
                Some(r) => r.iter().any(|r| r == recipient),
                None => true,
            })
            .filter(|share_cfg| share_cfg.name == share_name)
            .flat_map(|share_cfg| share_cfg.schemas())
            .map(|schema_cfg| schema_cfg.to_schema(share_name))
            .collect()
    }

    pub fn list_tables_in_share(&self, recipient: &str, share_name: &str) -> Vec<Table> {
        self.shares
            .iter()
            .filter(|share_cfg| match &share_cfg.recipients {
                Some(r) => r.iter().any(|r| r == recipient),
                None => true,
            })
            .filter(|share_cfg| share_cfg.name == share_name)
            .flat_map(|share_cfg| share_cfg.schemas())
            .flat_map(|schema_cfg| {
                std::iter::repeat(&schema_cfg.name).zip(schema_cfg.tables().iter())
            })
            .map(|(schema_name, table_cfg)| table_cfg.to_table(share_name, schema_name))
            .collect()
    }

    pub fn list_tables_in_schema(
        &self,
        recipient: &str,
        share_name: &str,
        schema_name: &str,
    ) -> Vec<Table> {
        self.shares
            .iter()
            .filter(|share_cfg| match &share_cfg.recipients {
                Some(r) => r.iter().any(|r| r == recipient),
                None => true,
            })
            .filter(|share_cfg| share_cfg.name == share_name)
            .flat_map(|share_cfg| share_cfg.schemas())
            .filter(|schema_cfg| schema_cfg.name == schema_name)
            .flat_map(|schema_cfg| schema_cfg.tables())
            .map(|table_cfg| table_cfg.to_table(share_name, schema_name))
            .collect()
    }

    pub fn get_share(&self, name: &str, recipient: &str) -> Option<Share> {
        self.list_shares(recipient)
            .into_iter()
            .find(|share| share.name() == name)
    }

    pub fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        recipient: &str,
    ) -> Option<Table> {
        self.list_tables_in_schema(recipient, share_name, schema_name)
            .into_iter()
            .find(|t| t.name() == table_name)
    }
}

impl Default for ShareFile {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ShareConfig {
    name: String,
    schemas: Vec<SchemaConfig>,
    recipients: Option<Vec<String>>,
    extensions: Option<HashMap<String, String>>,
}

impl ShareConfig {
    fn to_share(&self) -> Share {
        Share::builder()
            .name(&self.name)
            .set_extensions(self.extensions.clone())
            .build()
            .expect("valid share")
    }

    fn schemas(&self) -> &[SchemaConfig] {
        &self.schemas
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SchemaConfig {
    name: String,
    tables: Vec<TableConfig>,
}

impl SchemaConfig {
    fn tables(&self) -> &[TableConfig] {
        &self.tables
    }

    fn to_schema(&self, share_name: &str) -> Schema {
        Schema::builder()
            .name(&self.name)
            .share_name(share_name)
            .build()
            .expect("valid schema")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TableConfig {
    name: String,
    location: String,
    id: Option<String>,
    extensions: Option<HashMap<String, String>>,
}

impl TableConfig {
    fn to_table(&self, share_name: &str, schema_name: &str) -> Table {
        Table::builder()
            .name(&self.name)
            .storage_path(&self.location)
            .set_id(self.id.clone())
            .schema_name(schema_name)
            .share_name(share_name)
            .set_extensions(self.extensions.clone())
            .build()
            .expect("valid table")
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use crate::auth::RecipientId;

    use super::*;

    #[test]
    fn list_shares() {
        let json = json!({
            "shares": [
                {
                    "name": "share1",
                    "schemas": []
                },
                {
                    "name": "share2",
                    "schemas": [],
                    "recipients": ["client1"]
                },
                {
                    "name": "share3",
                    "schemas": [],
                    "recipients": []
                }
            ]
        });
        let file: ShareFile = serde_json::from_value(json).unwrap();

        let recipient = RecipientId::anonymous();
        assert_eq!(
            file.list_shares(recipient.as_ref())
                .into_iter()
                .map(|s| s.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["share1"]
        );

        let recipient = RecipientId::known("client1");
        assert_eq!(
            file.list_shares(recipient.as_ref())
                .into_iter()
                .map(|s| s.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["share1", "share2"]
        );

        let recipient = RecipientId::known("unauthorized-client");
        assert_eq!(
            file.list_shares(recipient.as_ref())
                .into_iter()
                .map(|s| s.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["share1"]
        );
    }

    #[test]
    fn get_share() {
        let json = json!({
            "shares": [
                {
                    "name": "share1",
                    "extensions": {"foo": "bar"},
                    "schemas": [],
                },
                {
                    "name": "share2",
                    "schemas": [],
                    "recipients": ["client1"]
                }
            ]
        });
        let file: ShareFile = serde_json::from_value(json).unwrap();

        let recipient = RecipientId::anonymous();
        let share = file.get_share("share1", recipient.as_ref()).unwrap();
        assert_eq!(share.name(), "share1");
        assert_eq!(share.get_extension("foo"), Some("bar"));
        assert_eq!(share.get_extension("?"), None);

        let recipient = RecipientId::known("unauthorized-client");
        let share = file.get_share("share2", recipient.as_ref());
        assert!(share.is_none());

        let recipient = RecipientId::known("client1");
        let share = file.get_share("share2", recipient.as_ref());
        assert!(share.is_some())
    }

    #[test]
    fn list_schemas() {
        let json = json!({
            "shares": [
                {
                    "name": "share1",
                    "schemas": [
                        {
                            "name": "schema1",
                            "tables": []
                        },
                        {
                            "name": "schema2",
                            "tables": []
                        }
                    ],
                }
            ]
        });
        let file: ShareFile = serde_json::from_value(json).unwrap();

        let recipient = RecipientId::anonymous();
        assert_eq!(
            file.list_schemas(recipient.as_ref(), "share1")
                .into_iter()
                .map(|s| s.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["schema1", "schema2"]
        );
    }

    #[test]
    fn list_tables_in_schema() {
        let json = json!({
            "shares": [
                {
                    "name": "share1",
                    "schemas": [
                        {
                            "name": "schema1",
                            "tables": [
                                {
                                    "name": "table1",
                                    "location": "s3://bucket/prefix1"
                                },
                                {
                                    "name": "table2",
                                    "location": "s3://bucket/prefix2"
                                }
                            ]
                        },
                        {
                            "name": "schema2",
                            "tables": [
                                {
                                    "name": "table3",
                                    "location": "s3://bucket/prefix3"
                                }
                            ]
                        }
                    ],
                }
            ]
        });
        let file: ShareFile = serde_json::from_value(json).unwrap();

        let recipient = RecipientId::anonymous();
        assert_eq!(
            file.list_tables_in_schema(recipient.as_ref(), "share1", "schema1")
                .into_iter()
                .map(|s| s.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["table1", "table2"]
        );

        let recipient = RecipientId::anonymous();
        assert_eq!(
            file.list_tables_in_schema(recipient.as_ref(), "share1", "schema2")
                .into_iter()
                .map(|s| s.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["table3"]
        );
    }

    #[test]
    fn list_tables_in_share() {
        let json = json!({
            "shares": [
                {
                    "name": "share1",
                    "schemas": [
                        {
                            "name": "schema1",
                            "tables": [
                                {
                                    "name": "table1",
                                    "location": "s3://bucket/prefix1"
                                },
                                {
                                    "name": "table2",
                                    "location": "s3://bucket/prefix2"
                                }
                            ]
                        },
                        {
                            "name": "schema2",
                            "tables": [
                                {
                                    "name": "table3",
                                    "location": "s3://bucket/prefix3"
                                }
                            ]
                        }
                    ],
                }
            ]
        });
        let file: ShareFile = serde_json::from_value(json).unwrap();

        let recipient = RecipientId::anonymous();
        assert_eq!(
            file.list_tables_in_share(recipient.as_ref(), "share1")
                .into_iter()
                .map(|s| s.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["table1", "table2", "table3"]
        );
    }

    #[test]
    fn get_table() {
        let json = json!({
            "shares": [
                {
                    "name": "share1",
                    "schemas": [
                        {
                            "name": "schema1",
                            "tables": [
                                {
                                    "name": "table1",
                                    "location": "s3://bucket/prefix1",
                                    "extensions": {"foo": "bar"}
                                },
                            ]
                        },
                    ],
                }
            ]
        });
        let file: ShareFile = serde_json::from_value(json).unwrap();

        let recipient = RecipientId::anonymous();
        let table = file
            .get_table("share1", "schema1", "table1", recipient.as_ref())
            .unwrap();
        assert_eq!(table.share_name(), "share1");
        assert_eq!(table.schema_name(), "schema1");
        assert_eq!(table.name(), "table1");
        assert_eq!(table.storage_path(), "s3://bucket/prefix1");
        assert_eq!(table.get_extension("foo"), Some("bar"));
        assert_eq!(table.get_extension("?"), None);

        let table = file.get_table("share1", "schema1", "?", recipient.as_ref());
        assert!(table.is_none());
    }
}
