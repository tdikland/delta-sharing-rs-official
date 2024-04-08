//! Types and traits for accessing shared assets.
//!
//! Every Delta Sharing server needs to know which shares, schemas and tables
//! are available to be shared to the recipient. The server also needs to know
//! where the specific assets are stored. This module provides the traits and
//! types that are used by the sharing server to list and get information about
//! the shared assets.
//!
//! The [`Catalog`] trait is implemented by the different share managers
//! that each may represent a different backing store for the shared assets.
//! The [`Catalog`] trait provides methods to list shares, schemas and
//! tables and to get details about a specific share or table. The
//! [`RecipientId`] type is used to identify the recipient that is querying the
//! shared assets. Based on the passed [`RecipientId`] the share manager can
//! decide which shares, schemas and tables are available to the recipient.

#![warn(missing_docs)]

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, fmt::Display};

use crate::auth::RecipientId;

pub mod file;

/// Interface for listing and reading shared assets in the Delta Sharing server.
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Return a page of shares stored on the sharing server store accessible
    /// to the given recipient. The pagination argument is used to limit the
    /// amount of returned shares in this call and to resume listing from a
    /// specified point in the collection of shares.
    async fn list_shares(
        &self,
        recipient_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<Page<Share>, CatalogError>;

    /// Return a page of schemas stored on the sharing server store that belong
    /// to the specified share if accessible to the given recipient. The
    /// pagination argument is used to limit the amount of returned schemas in
    /// this call and to resume listing from a specified point in the
    /// collection of schemas.
    async fn list_schemas(
        &self,
        share_name: &str,
        recipient_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<Page<Schema>, CatalogError>;

    /// Return a page of tables stored on the sharing server store that belong
    /// to the specified share if accessible to the given recipient. The
    /// pagination argument is used to limit the amount of returned tables in
    /// this call and to resume listing from a specified point in the
    /// collection of tables.
    async fn list_tables_in_share(
        &self,
        share_name: &str,
        recipient_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<Page<Table>, CatalogError>;

    /// Return a page of tables stored on the sharing server store that belong
    /// to the specified share+schema and are accessible to the given recipient.
    /// The pagination argument is used to limit the amount of returned tables
    /// in this call and to resume listing from a specified point in the
    /// collection of tables.
    async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
        recipient_id: &RecipientId,
        pagination: &Pagination,
    ) -> Result<Page<Table>, CatalogError>;

    /// Return a share with the specified name if it is accessible to the
    /// given recipient.
    async fn get_share(
        &self,
        share_name: &str,
        recipient_id: &RecipientId,
    ) -> Result<Share, CatalogError>;

    /// Return a table with the specified name within the specified share and
    /// schema if it is accessible to the given recipient.
    async fn get_table(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        recipient_id: &RecipientId,
    ) -> Result<Table, CatalogError>;
}

/// Pagination parameters for listing shared assets.
#[derive(Debug, Clone, Default, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    max_results: Option<u32>,
    page_token: Option<String>,
}

impl Pagination {
    /// Create a new pagination struct with the specified maximum results
    /// per page and continuation token.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Pagination;
    ///
    /// let pagination = Pagination::new(Some(43), Some("foo".to_string()));
    /// assert_eq!(pagination.max_results(), Some(43));
    /// assert_eq!(pagination.page_token(), Some("foo"));
    /// ```
    pub fn new(max_results: Option<u32>, page_token: Option<String>) -> Self {
        Self {
            max_results,
            page_token,
        }
    }

    /// Return the maximum amount of results to be returned in a single page.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Pagination;
    ///
    /// let pagination = Pagination::new(Some(43), None);
    /// assert_eq!(pagination.max_results(), Some(43));
    /// ```
    pub fn max_results(&self) -> Option<u32> {
        self.max_results
    }

    /// Return the token that can be used to resume listing from the specified
    /// point in the collection of shared assets.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Pagination;
    ///
    /// let pagination = Pagination::new(None, Some("foo".to_string()));
    /// assert_eq!(pagination.page_token(), Some("foo"));
    /// ```
    pub fn page_token(&self) -> Option<&str> {
        self.page_token.as_deref()
    }
}

/// A page of shared assets returned from the [`Catalog`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    items: Vec<T>,
    next_page_token: Option<String>,
}

impl<T> Page<T> {
    /// Create a new page with the specified items and token.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::builder().name("foo").build().unwrap(),
    ///     Share::builder().name("bar").build().unwrap()
    /// ];
    /// let page = Page::new(shares, Some("token".to_string()));
    /// assert_eq!(page.len(), 2);
    /// assert_eq!(page.next_page_token(), Some("token"));
    /// ```
    pub fn new(items: Vec<T>, next_page_token: Option<String>) -> Self {
        Self {
            items,
            next_page_token,
        }
    }

    /// Return the shared assets in the page.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::builder().name("foo").build().unwrap(),
    ///     Share::builder().name("bar").build().unwrap()
    /// ];
    /// let page = Page::new(shares.clone(), None);
    /// assert_eq!(page.items(), &shares);
    /// ````
    pub fn items(&self) -> &Vec<T> {
        &self.items
    }

    /// Return the token that can be used to resume listing from a specified
    /// point in the collection of shared assets.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::builder().name("foo").build().unwrap(),
    ///     Share::builder().name("bar").build().unwrap()
    /// ];
    /// let page = Page::new(shares, Some("token".to_string()));
    /// assert_eq!(page.next_page_token(), Some("token"));
    /// ```
    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }

    /// Return the amount of shared assets in the page.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::builder().name("foo").build().unwrap(),
    ///     Share::builder().name("bar").build().unwrap()
    /// ];
    /// let page = Page::new(shares, None);
    /// assert_eq!(page.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Return whether the page is empty.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::{Page, Share};
    ///
    /// let page = Page::<Share>::new(vec![], None);
    /// assert!(page.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Return whether the page is the last page of the collection.
    ///
    /// If the next page token is `None`, then the page is the last page.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::builder().name("foo").build().unwrap(),
    ///     Share::builder().name("bar").build().unwrap()
    /// ];
    /// let page = Page::new(shares, None);
    /// assert!(page.is_last_page());
    /// ```
    pub fn is_last_page(&self) -> bool {
        self.next_page_token.is_none()
    }

    /// Convert the page into its parts: items and token.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::{Page, Share};
    ///
    /// let shares = vec![
    ///     Share::builder().name("foo").build().unwrap(),
    ///     Share::builder().name("bar").build().unwrap()
    /// ];
    /// let page = Page::new(shares, Some("token".to_string()));
    /// let (items, token) = page.into_parts();
    /// assert_eq!(items.len(), 2);
    /// assert_eq!(token, Some("token".to_string()));
    /// ```
    pub fn into_parts(self) -> (Vec<T>, Option<String>) {
        (self.items, self.next_page_token)
    }
}

/// Information about a share stored in the [`Catalog`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Share {
    id: Option<String>,
    name: String,
    extensions: Option<HashMap<String, String>>,
}

impl Share {
    /// Create a new [`ShareBuilder`]
    pub fn builder() -> ShareBuilder {
        ShareBuilder::new()
    }

    /// Return the id of the share.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Share;
    ///
    /// let share = Share::builder().name("foo").id("bar").build().unwrap();
    /// assert_eq!(share.id(), Some("bar"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Return the name of the share.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Share;
    ///
    /// let share = Share::builder().name("foo").build().unwrap();
    /// assert_eq!(share.name(), "foo");
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to the share extension corresponding to the key.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Share;
    ///
    /// let share = Share::builder().name("foo").add_extension("bar", "baz").build().unwrap();
    /// assert_eq!(share.get_extension("bar"), Some("baz"));
    /// assert_eq!(share.get_extension("does-not-exist"), None);
    /// ```
    pub fn get_extension(&self, key: &str) -> Option<&str> {
        self.extensions
            .as_ref()
            .and_then(|ex| ex.get(key).map(|v| v.as_ref()))
    }
}

/// A builder for the [`Share`] type
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ShareBuilder {
    id: Option<String>,
    name: Option<String>,
    extensions: Option<HashMap<String, String>>,
}

impl ShareBuilder {
    /// Create a new [`ShareBuilder`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the id of the share
    pub fn id(mut self, share_id: impl Into<String>) -> Self {
        self.id = Some(share_id.into());
        self
    }

    /// Set the id of the share
    pub fn set_id(mut self, share_id: Option<String>) -> Self {
        self.id = share_id;
        self
    }

    /// Set the name of the share
    pub fn name(mut self, share_name: impl Into<String>) -> Self {
        self.name = Some(share_name.into());
        self
    }

    /// Set the name of the share
    pub fn set_name(mut self, share_name: Option<String>) -> Self {
        self.name = share_name;
        self
    }

    /// Set share extension key-value pair
    pub fn add_extension(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        self.extensions
            .get_or_insert_with(HashMap::new)
            .insert(key, value);
        self
    }

    /// Set share extensions
    pub fn set_extensions(mut self, extensions: Option<HashMap<String, String>>) -> Self {
        self.extensions = extensions;
        self
    }

    /// Build the share
    pub fn build(self) -> Result<Share, CatalogError> {
        let Some(share_name) = self.name else {
            return Err(CatalogError::internal(
                "the required attribute `name` was not set",
            ));
        };

        Ok(Share {
            id: self.id,
            name: share_name,
            extensions: self.extensions,
        })
    }
}

/// Information about a schema stored on the sharing server store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    id: Option<String>,
    name: String,
    share_name: String,
    extensions: Option<HashMap<String, String>>,
}

impl Schema {
    /// Create a new [`SchemaBuilder`]
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// Return a reference to the id of the schema.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Schema;
    ///
    /// let schema = Schema::builder()
    ///     .share_name("foo")
    ///     .name("bar")
    ///     .id("baz")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(schema.id(), Some("baz"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Return a reference to the name of the schema.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Schema;
    ///
    /// let schema = Schema::builder()
    ///     .share_name("foo")
    ///     .name("bar")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(schema.name(), "bar");
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return a reference to the the name of the share containing the schema.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Schema;
    ///
    /// let schema = Schema::builder()
    ///     .share_name("foo")
    ///     .name("bar")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(schema.share_name(), "foo");
    /// ```
    pub fn share_name(&self) -> &str {
        &self.share_name
    }
}

/// A builder for the [`Schema`] type
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaBuilder {
    schema_id: Option<String>,
    share_id: Option<String>,
    schema_name: Option<String>,
    share_name: Option<String>,
    extensions: Option<HashMap<String, String>>,
}

impl SchemaBuilder {
    /// Create a new [`SchemaBuilder`]
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the id of the schema
    pub fn id(mut self, schema_id: impl Into<String>) -> Self {
        self.schema_id = Some(schema_id.into());
        self
    }

    /// Set the id of the schema
    pub fn set_id(mut self, schema_id: Option<String>) -> Self {
        self.schema_id = schema_id;
        self
    }

    /// Set the share_id of the share containing the schema
    pub fn share_id(mut self, share_id: impl Into<String>) -> Self {
        self.share_id = Some(share_id.into());
        self
    }

    /// Set the share_id of the share containing the schema
    pub fn set_share_id(mut self, share_id: Option<String>) -> Self {
        self.share_id = share_id;
        self
    }

    /// Set the name of the schema
    pub fn name(mut self, schema_name: impl Into<String>) -> Self {
        self.schema_name = Some(schema_name.into());
        self
    }

    /// Set the name of the schema
    pub fn set_name(mut self, schema_name: Option<String>) -> Self {
        self.schema_name = schema_name;
        self
    }

    /// Set the name of the share containing the schema
    pub fn share_name(mut self, share_name: impl Into<String>) -> Self {
        self.share_name = Some(share_name.into());
        self
    }

    /// Set the name of the share containing the schema
    pub fn set_share_name(mut self, share_name: Option<String>) -> Self {
        self.share_name = share_name;
        self
    }

    /// Build the schema
    pub fn build(self) -> Result<Schema, CatalogError> {
        let Some(schema_name) = self.schema_name else {
            return Err(CatalogError::internal(
                "the required attribute `name` was not set",
            ));
        };

        let Some(share_name) = self.share_name else {
            return Err(CatalogError::internal(
                "the required attribute `share_name` was not set",
            ));
        };

        Ok(Schema {
            id: self.schema_id,
            name: schema_name,
            share_name,
            extensions: self.extensions,
        })
    }
}

/// Information about a table stored on the sharing server store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    id: Option<String>,
    name: String,
    share_id: Option<String>,
    share_name: String,
    schema_name: String,
    storage_location: String,
    extensions: Option<HashMap<String, String>>,
}

impl Table {
    /// Create a new [`TableBuilder`]
    pub fn builder() -> TableBuilder {
        TableBuilder::new()
    }

    /// Return a reference to the table id.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Table;
    ///
    /// let table = Table::builder()
    ///     .share_name("foo")
    ///     .schema_name("bar")
    ///     .name("baz")
    ///     .storage_path("s3://bucket/prefix/")
    ///     .id("qux")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(table.id(), Some("qux"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Return a reference to the id of the share containing the table.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Table;
    ///
    /// let table = Table::builder()
    ///     .share_name("foo")
    ///     .schema_name("bar")
    ///     .name("baz")
    ///     .storage_path("s3://bucket/prefix/")
    ///     .share_id("qux")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(table.share_id(), Some("qux"));
    /// ```
    pub fn share_id(&self) -> Option<&str> {
        self.share_id.as_deref()
    }

    /// Return a reference to the table name.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Table;
    ///
    /// let table = Table::builder()
    ///     .share_name("foo")
    ///     .schema_name("bar")
    ///     .name("baz")
    ///     .storage_path("s3://bucket/prefix/")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(table.name(), "baz");
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return a reference to the name of the schema containing the table.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Table;
    ///
    /// let table = Table::builder()
    ///     .share_name("foo")
    ///     .schema_name("bar")
    ///     .name("baz")
    ///     .storage_path("s3://bucket/prefix/")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(table.schema_name(), "bar");
    /// ```
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    /// Return a reference to the name of the share containing the table.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Table;
    ///
    /// let table = Table::builder()
    ///     .share_name("foo")
    ///     .schema_name("bar")
    ///     .name("baz")
    ///     .storage_path("s3://bucket/prefix/")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(table.share_name(), "foo");
    /// ```
    pub fn share_name(&self) -> &str {
        &self.share_name
    }

    /// Return a reference to the storage path of the table.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Table;
    ///
    /// let table = Table::builder()
    ///     .share_name("foo")
    ///     .schema_name("bar")
    ///     .name("baz")
    ///     .storage_path("s3://bucket/prefix/")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(table.storage_path(), "s3://bucket/prefix/");
    /// ```
    pub fn storage_path(&self) -> &str {
        &self.storage_location
    }

    /// Returns a reference to the table extension corresponding to the key.
    ///
    /// # Example
    /// ```rust
    /// use delta_sharing::catalog::Table;
    ///
    /// let table = Table::builder()
    ///     .share_name("foo")
    ///     .schema_name("bar")
    ///     .name("baz")
    ///     .storage_path("s3://bucket/prefix/")
    ///     .add_extension("qux", "quux")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(table.get_extension("qux"), Some("quux"));
    /// assert_eq!(table.get_extension("does-not-exist"), None);
    /// ```
    pub fn get_extension(&self, key: &str) -> Option<&str> {
        self.extensions.as_ref()?.get(key).map(|s| s.as_str())
    }
}

/// A builder for the [`Table`] type
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableBuilder {
    share_id: Option<String>,
    schema_id: Option<String>,
    table_id: Option<String>,
    share_name: Option<String>,
    schema_name: Option<String>,
    table_name: Option<String>,
    storage_path: Option<String>,
    extensions: Option<HashMap<String, String>>,
}

impl TableBuilder {
    /// Create a new [`TableBuilder`]
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the id of the share containing the table
    pub fn share_id(mut self, share_id: impl Into<String>) -> Self {
        self.share_id = Some(share_id.into());
        self
    }

    /// Set the id of the share containing the table
    pub fn set_share_id(mut self, share_id: Option<String>) -> Self {
        self.share_id = share_id;
        self
    }

    /// Set the id of the schema containing the table
    pub fn schema_id(mut self, schema_id: impl Into<String>) -> Self {
        self.schema_id = Some(schema_id.into());
        self
    }

    /// Set the id of the schema containing the table
    pub fn set_schema_id(mut self, schema_id: Option<String>) -> Self {
        self.schema_id = schema_id;
        self
    }

    /// Set the id of the table
    pub fn id(mut self, table_id: impl Into<String>) -> Self {
        self.table_id = Some(table_id.into());
        self
    }

    /// Set the id of the table
    pub fn set_id(mut self, table_id: Option<String>) -> Self {
        self.table_id = table_id;
        self
    }

    /// Set the name of the share containing the table
    pub fn share_name(mut self, share_name: impl Into<String>) -> Self {
        self.share_name = Some(share_name.into());
        self
    }

    /// Set the name of the share containing the table
    pub fn set_share_name(mut self, share_name: Option<String>) -> Self {
        self.share_name = share_name;
        self
    }

    /// Set the name of the schema containing the table
    pub fn schema_name(mut self, schema_name: impl Into<String>) -> Self {
        self.schema_name = Some(schema_name.into());
        self
    }

    /// Set the name of the schema containing the table
    pub fn set_schema_name(mut self, schema_name: Option<String>) -> Self {
        self.schema_name = schema_name;
        self
    }

    /// Set the name of the table
    pub fn name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    /// Set the name of the table
    pub fn set_name(mut self, table_name: Option<String>) -> Self {
        self.table_name = table_name;
        self
    }

    /// Set the storage location of the table
    pub fn storage_path(mut self, path: impl Into<String>) -> Self {
        self.storage_path = Some(path.into());
        self
    }

    /// Set the storage location of the table
    pub fn set_storage_path(mut self, path: Option<String>) -> Self {
        self.storage_path = path;
        self
    }

    /// Add table extension key value pair
    pub fn add_extension(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        self.extensions
            .get_or_insert_with(HashMap::new)
            .insert(key, value);
        self
    }

    /// Set the table extensions
    pub fn set_extensions(mut self, extensions: Option<HashMap<String, String>>) -> Self {
        self.extensions = extensions;
        self
    }

    /// Build the table
    pub fn build(self) -> Result<Table, CatalogError> {
        let Some(share_name) = self.share_name else {
            return Err(CatalogError::internal(
                "the required attribute `share_name` was not set",
            ));
        };

        let Some(schema_name) = self.schema_name else {
            return Err(CatalogError::internal(
                "the required attribute `schema_name` was not set",
            ));
        };

        let Some(table_name) = self.table_name else {
            return Err(CatalogError::internal(
                "the required attribute `table_name` was not set",
            ));
        };

        let Some(storage_path) = self.storage_path else {
            return Err(CatalogError::internal(
                "the required attribute `storage_path` was not set",
            ));
        };

        Ok(Table {
            id: self.table_id,
            name: table_name,
            share_id: self.share_id,
            share_name,
            schema_name,
            storage_location: storage_path,
            extensions: self.extensions,
        })
    }
}

/// Errors that can occur during the listing and retrieval of shared assets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CatalogErrorKind {
    /// The requested share or table was not found.
    ResourceNotFound,
    /// The requested share or table is forbidden for the recipient.
    ResourceForbidden,
    /// The pagination token is malformed.
    MalformedPagination,
    /// The [`Catalog`] has an internal error.
    Internal,
}

/// Error that occurred during the listing and retrieval of shared assets.
///
/// This error is used to wrap the specific error that occurred and to provide
/// a message that can be used to describe the error.
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
pub struct CatalogError {
    kind: CatalogErrorKind,
    message: String,
}

impl CatalogError {
    /// Create a new error with the specified kind and message.
    pub fn new(kind: CatalogErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Return the kind of the error.
    pub fn kind(&self) -> CatalogErrorKind {
        self.kind
    }

    /// Return the message of the error.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Create a new error indicating that the requested share or table was not
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(CatalogErrorKind::ResourceNotFound, message)
    }

    /// Create a new error indicating that the requested share or table is
    /// forbidden.
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(CatalogErrorKind::ResourceForbidden, message)
    }

    /// Create a new error indicating that the pagination token is malformed.
    pub fn malformed_pagination(message: impl Into<String>) -> Self {
        Self::new(CatalogErrorKind::MalformedPagination, message)
    }

    /// Create a new error indicating that the [`Catalog`] has an internal
    /// error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(CatalogErrorKind::Internal, message)
    }
}

impl Display for CatalogErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogErrorKind::ResourceNotFound => write!(f, "NOT_FOUND"),
            CatalogErrorKind::ResourceForbidden => write!(f, "FORBIDDEN"),
            CatalogErrorKind::MalformedPagination => write!(f, "MALFORMED_PAGINATION"),
            CatalogErrorKind::Internal => write!(f, "INTERNAL_ERROR"),
        }
    }
}

impl Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind, self.message)
    }
}

impl Error for CatalogError {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn share_builder() {
        let share = Share::builder()
            .id("id")
            .name("name")
            .build()
            .expect("failed to build share");

        assert_eq!(share.id(), Some("id"));
        assert_eq!(share.name(), "name");

        let share = Share::builder().id("id").build();
        assert!(share.is_err());
        assert_eq!(share.unwrap_err().kind(), CatalogErrorKind::Internal);
    }

    #[test]
    fn schema_builder() {
        let schema = Schema::builder()
            .id("id")
            .name("name")
            .share_name("share")
            .build()
            .expect("failed to build schema");

        assert_eq!(schema.id(), Some("id"));
        assert_eq!(schema.name(), "name");
        assert_eq!(schema.share_name(), "share");

        let schema = Schema::builder().id("id").build();
        assert!(schema.is_err());
        assert_eq!(schema.unwrap_err().kind(), CatalogErrorKind::Internal);
    }

    #[test]
    fn table_builder() {
        let tb = Table::builder()
            .id("table_id")
            .name("table_name")
            .share_id("share_id")
            .share_name("share_name")
            .schema_name("schema_name")
            .storage_path("path")
            .add_extension("foo", "bar");

        let good_table = tb.clone().build().expect("failed to build table");
        assert_eq!(good_table.id(), Some("table_id"));
        assert_eq!(good_table.share_id(), Some("share_id"));
        assert_eq!(good_table.name(), "table_name");
        assert_eq!(good_table.share_name(), "share_name");
        assert_eq!(good_table.schema_name(), "schema_name");
        assert_eq!(good_table.storage_path(), "path");
        assert_eq!(good_table.get_extension("foo"), Some("bar"));
        assert_eq!(good_table.get_extension("not-existing-key"), None);

        let missing_table_name = tb.clone().set_name(None).build();
        assert!(missing_table_name.is_err());
        assert_eq!(
            missing_table_name.unwrap_err().kind(),
            CatalogErrorKind::Internal
        );

        let missing_schema_name = tb.clone().set_schema_name(None).build();
        assert!(missing_schema_name.is_err());
        assert_eq!(
            missing_schema_name.unwrap_err().kind(),
            CatalogErrorKind::Internal
        );

        let missing_share_name = tb.clone().set_share_name(None).build();
        assert!(missing_share_name.is_err());
        assert_eq!(
            missing_share_name.unwrap_err().kind(),
            CatalogErrorKind::Internal
        );

        let missing_table_path = tb.clone().set_storage_path(None).build();
        assert!(missing_table_path.is_err());
        assert_eq!(
            missing_table_path.unwrap_err().kind(),
            CatalogErrorKind::Internal
        );
    }
}