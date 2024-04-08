//! Catalog implementation based on a configuration file.
//!
//! The file based catalog must be initialized using a [`FileCatalogConfig`]
//! that specifies the path on the local filesystem to the configuration file
//! as well the specific file format. The following formats are supported:
//! YAML, JSON, TOML.
//!
//! # Example
//! ```rust
//! let cfg = FileCatalogConfig::new("/tmp/path/to/catalog/cfg.yaml");
//! let catalog = FileCatalog::new(cfg)
//!
//! let shares = catalog.list_shares().await;
//! ```

use self::model::ShareFile;

use crate::auth::RecipientId;
use crate::catalog::{Catalog, CatalogError, Page, Pagination, Schema, Share, Table};

mod config;
mod model;

pub use config::{FileCatalogConfig, FileFormat};

/// Catalog based on a configuration file.
#[derive(Debug, Clone, PartialEq)]
pub struct FileCatalog {
    config: FileCatalogConfig,
    shares: ShareFile,
}

impl FileCatalog {
    /// Creates a new instance of the FileShareManager.
    pub fn new(config: FileCatalogConfig) -> Self {
        let mut this = Self {
            config,
            shares: Default::default(),
        };
        this.load().expect("configuration file could not be loaded");
        this
    }

    fn load(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let handle = std::fs::OpenOptions::new()
            .read(true)
            .open(self.config.path())?;

        let shares: ShareFile = match self.config.format() {
            config::FileFormat::Json => serde_json::from_reader(handle)?,
            config::FileFormat::Yaml => serde_yaml::from_reader(handle)?,
            config::FileFormat::Toml => {
                let content = std::fs::read_to_string(self.config.path())?;
                toml::from_str(&content)?
            }
        };
        self.shares = shares;

        Ok(())
    }

    fn file(&self) -> &ShareFile {
        &self.shares
    }
}

#[async_trait::async_trait]
impl Catalog for FileCatalog {
    async fn list_shares(
        &self,
        recipient_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<Page<Share>, CatalogError> {
        let shares = self.file().list_shares(recipient_id.as_ref());
        paginate_response(shares, pagination)
    }

    async fn get_share(
        &self,
        share_name: &str,
        recipient_id: &RecipientId,
    ) -> Result<Share, CatalogError> {
        self.file()
            .get_share(share_name, recipient_id.as_ref())
            .ok_or(CatalogError::not_found(""))
    }

    async fn list_schemas(
        &self,
        share_name: &str,
        recipient_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<Page<Schema>, CatalogError> {
        let schemas = self.file().list_schemas(recipient_id.as_ref(), share_name);
        paginate_response(schemas, pagination)
    }

    async fn list_tables_in_share(
        &self,
        share_name: &str,
        recipient_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<Page<Table>, CatalogError> {
        let tables = self
            .file()
            .list_tables_in_share(recipient_id.as_ref(), share_name);
        paginate_response(tables, pagination)
    }

    async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        recipient_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<Page<Table>, CatalogError> {
        let tables =
            self.file()
                .list_tables_in_schema(recipient_id.as_ref(), share_name, schema_name);
        paginate_response(tables, pagination)
    }

    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        recipient_id: &RecipientId,
    ) -> Result<Table, CatalogError> {
        self.file()
            .list_tables_in_schema(recipient_id.as_ref(), share_name, schema_name)
            .into_iter()
            .find(|table| table.name() == table_name)
            .ok_or(CatalogError::not_found("table not found"))
    }
}

fn paginate_response<T: Clone>(
    items: Vec<T>,
    pagination: &Pagination,
) -> Result<Page<T>, CatalogError> {
    let offset = pagination
        .page_token()
        .map(|token| {
            token.parse::<usize>().map_err({
                |e| {
                    tracing::error!(pagination = ?token, error = ?e, "invalid page token");
                    CatalogError::malformed_pagination("Invalid page token")
                }
            })
        })
        .transpose()?
        .unwrap_or(0);
    let max_results = pagination.max_results().unwrap_or(500) as usize;

    let page = if offset + max_results >= items.len() {
        Page::new(items[offset..].to_vec(), None)
    } else {
        Page::new(
            items[offset..offset + max_results].to_vec(),
            Some((offset + max_results).to_string()),
        )
    };

    Ok(page)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    fn setup_share_config_file() -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().unwrap();
        let shares_config = r#"shares:
- name: "share1"
  schemas:
  - name: "schema1"
    tables:
    - name: "table1"
      location: "s3a://<bucket-name>/<the-table-path>"
      id: "00000000-0000-0000-0000-000000000000"
    - name: "table2"
      location: "wasbs://<container-name>@<account-name}.blob.core.windows.net/<the-table-path>"
      id: "00000000-0000-0000-0000-000000000001"
- name: "share2"
  schemas:
  - name: "schema2"
    tables:
    - name: "table3"
      location: "abfss://<container-name>@<account-name}.dfs.core.windows.net/<the-table-path>"
      cdfEnabled: true
      id: "00000000-0000-0000-0000-000000000002"
- name: "share3"
  schemas:
  - name: "schema3"
    tables:
    - name: "table4"
      location: "gs://<bucket-name>/<the-table-path>"
      id: "00000000-0000-0000-0000-000000000003"
- name: "share4"
  schemas:
  - name: "schema4"
    tables:
    - name: "table5"
      location: "s3a://<bucket-name>/<the-table-path>"
      id: "00000000-0000-0000-0000-000000000004""#;
        temp_file.write_all(shares_config.as_bytes()).unwrap();
        temp_file
    }

    #[tokio::test]
    async fn list_shares() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);

        let page = catalog
            .list_shares(&RecipientId::anonymous(), &Pagination::new(Some(2), None))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.next_page_token(), Some("2"));

        let page = catalog
            .list_shares(
                &RecipientId::anonymous(),
                &Pagination::new(Some(2), Some("2".to_string())),
            )
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.next_page_token(), None);
    }

    #[tokio::test]
    async fn get_share() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let share = catalog
            .get_share("share1", &RecipientId::anonymous())
            .await
            .unwrap();
        assert_eq!(share.name(), "share1");
    }

    #[tokio::test]
    async fn list_schemas() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let schemas = catalog
            .list_schemas("share1", &RecipientId::anonymous(), &Pagination::default())
            .await
            .unwrap();
        assert_eq!(schemas.items.len(), 1);
        assert_eq!(schemas.items[0].name(), "schema1");
        assert_eq!(schemas.items[0].share_name(), "share1");
        assert_eq!(schemas.items[0].id(), None);
    }

    #[tokio::test]
    async fn list_tables_in_share() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let tables = catalog
            .list_tables_in_share("share1", &RecipientId::anonymous(), &Pagination::default())
            .await
            .unwrap();
        assert_eq!(tables.items.len(), 2);
        assert_eq!(tables.items[0].name(), "table1");
        assert_eq!(tables.items[0].schema_name(), "schema1");
        assert_eq!(tables.items[0].share_name(), "share1");
        assert_eq!(
            tables.items[0].storage_path(),
            "s3a://<bucket-name>/<the-table-path>"
        );
    }

    #[tokio::test]
    async fn list_tables_in_schema() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let tables = catalog
            .list_tables_in_schema(
                "share1",
                "schema1",
                &RecipientId::anonymous(),
                &Pagination::default(),
            )
            .await
            .unwrap();
        assert_eq!(tables.items.len(), 2);
        assert_eq!(tables.items[0].name(), "table1");
        assert_eq!(tables.items[0].schema_name(), "schema1");
        assert_eq!(tables.items[0].share_name(), "share1");
        assert_eq!(
            tables.items[0].storage_path(),
            "s3a://<bucket-name>/<the-table-path>"
        );
    }

    #[tokio::test]
    async fn get_table() {
        let tempfile = setup_share_config_file();
        let config = FileCatalogConfig::new(tempfile.path());

        let catalog = FileCatalog::new(config);
        let tables = catalog
            .get_table("share1", "schema1", "table1", &RecipientId::anonymous())
            .await
            .unwrap();
        assert_eq!(tables.name(), "table1");
        assert_eq!(tables.schema_name(), "schema1");
        assert_eq!(tables.share_name(), "share1");
        assert_eq!(
            tables.storage_path(),
            "s3a://<bucket-name>/<the-table-path>"
        );
        assert_eq!(tables.id(), Some("00000000-0000-0000-0000-000000000000"));
        assert_eq!(tables.share_id(), None);
    }
}
