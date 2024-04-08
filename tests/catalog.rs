use std::collections::HashSet;

use delta_sharing::{
    auth::RecipientId,
    catalog::{
        file::{FileCatalog, FileCatalogConfig, FileFormat},
        Catalog, CatalogErrorKind, Pagination,
    },
};

#[tokio::test]
async fn yaml_file_catalog() {
    let cfg = FileCatalogConfig::new("./tests/fixtures/yaml_catalog_file.yaml");
    let catalog = FileCatalog::new(cfg);

    list_shares(&catalog).await;
    list_shares_pagination(&catalog).await;
    list_shares_acl(&catalog).await;
    list_schemas(&catalog).await;
    list_tables_in_share(&catalog).await;
    list_tables_in_schema(&catalog).await;
    get_share(&catalog).await;
    get_table(&catalog).await;
}

#[tokio::test]
async fn json_file_catalog() {
    let cfg = FileCatalogConfig::new("./tests/fixtures/json_catalog_file.json")
        .with_format(FileFormat::Json);
    let catalog = FileCatalog::new(cfg);

    list_shares(&catalog).await;
    list_shares_pagination(&catalog).await;
    list_shares_acl(&catalog).await;
    list_schemas(&catalog).await;
    list_tables_in_share(&catalog).await;
    list_tables_in_schema(&catalog).await;
    get_share(&catalog).await;
    get_table(&catalog).await;
}

#[tokio::test]
async fn toml_file_catalog() {
    let cfg = FileCatalogConfig::new("./tests/fixtures/toml_catalog_file.toml")
        .with_format(FileFormat::Toml);
    let catalog = FileCatalog::new(cfg);

    list_shares(&catalog).await;
    list_shares_pagination(&catalog).await;
    list_shares_acl(&catalog).await;
    list_schemas(&catalog).await;
    list_tables_in_share(&catalog).await;
    list_tables_in_schema(&catalog).await;
    get_share(&catalog).await;
    get_table(&catalog).await;
}

async fn list_shares<C: Catalog>(catalog: &C) {
    let recipient = RecipientId::anonymous();
    let pagination = Pagination::default();
    let shares = catalog.list_shares(&recipient, &pagination).await.unwrap();

    let share_names = shares
        .items()
        .into_iter()
        .map(|s| s.name())
        .collect::<HashSet<_>>();

    assert_eq!(
        share_names,
        HashSet::from_iter(["public_share_1", "public_share_2", "public_share_3"])
    );
}

async fn list_shares_pagination<C: Catalog>(catalog: &C) {
    let recipient = RecipientId::anonymous();
    let pagination = Pagination::new(Some(2), None);
    let shares_page_1 = catalog.list_shares(&recipient, &pagination).await.unwrap();
    assert_eq!(shares_page_1.len(), 2);
    assert!(shares_page_1.next_page_token().is_some());

    let page_token = shares_page_1.next_page_token().unwrap().to_owned();
    let pagination = Pagination::new(Some(2), Some(page_token));
    let shares_page_2 = catalog.list_shares(&recipient, &pagination).await.unwrap();
    assert_eq!(shares_page_2.len(), 1);
    assert!(shares_page_2.next_page_token().is_none());

    let share_names = shares_page_1
        .items()
        .iter()
        .chain(shares_page_2.items().iter())
        .map(|s| s.name())
        .collect::<HashSet<_>>();

    assert_eq!(
        share_names,
        HashSet::from_iter(["public_share_1", "public_share_2", "public_share_3"])
    );
}

async fn list_shares_acl<C: Catalog>(catalog: &C) {
    let recipient = RecipientId::known("recipient1");
    let pagination = Pagination::default();
    let shares = catalog.list_shares(&recipient, &pagination).await.unwrap();

    let share_names = shares
        .items()
        .into_iter()
        .map(|s| s.name())
        .collect::<HashSet<_>>();

    assert_eq!(
        share_names,
        HashSet::from_iter([
            "public_share_1",
            "public_share_2",
            "public_share_3",
            "private_share_1"
        ])
    );
}

async fn list_schemas<C: Catalog>(catalog: &C) {
    let recipient = RecipientId::anonymous();
    let pagination = Pagination::default();

    let schemas = catalog
        .list_schemas("public_share_1", &recipient, &pagination)
        .await
        .unwrap();
    let schema_names = schemas
        .items()
        .into_iter()
        .map(|s| s.name())
        .collect::<HashSet<_>>();
    assert_eq!(schema_names, HashSet::from_iter(["schema1", "schema2"]));

    let page = catalog
        .list_schemas("not-existing-share", &recipient, &pagination)
        .await
        .unwrap();
    assert!(page.is_empty());
}

async fn list_tables_in_share<C: Catalog>(catalog: &C) {
    let recipient = RecipientId::anonymous();
    let pagination = Pagination::default();

    let tables = catalog
        .list_tables_in_share("public_share_1", &recipient, &pagination)
        .await
        .unwrap();
    let table_names = tables
        .items()
        .into_iter()
        .map(|s| s.name())
        .collect::<HashSet<_>>();
    assert_eq!(
        table_names,
        HashSet::from_iter(["table1", "table2", "table3"])
    );
}

async fn list_tables_in_schema<C: Catalog>(catalog: &C) {
    let recipient = RecipientId::anonymous();
    let pagination = Pagination::default();

    let tables = catalog
        .list_tables_in_schema("public_share_1", "schema1", &recipient, &pagination)
        .await
        .unwrap();
    let table_names = tables
        .items()
        .into_iter()
        .map(|s| s.name())
        .collect::<HashSet<_>>();
    assert_eq!(table_names, HashSet::from_iter(["table1", "table2"]));
}

async fn get_share<C: Catalog>(catalog: &C) {
    let recipient = RecipientId::anonymous();

    let share = catalog
        .get_share("public_share_1", &recipient)
        .await
        .unwrap();
    assert_eq!(share.name(), "public_share_1");
    assert_eq!(share.get_extension("?"), None);

    let recipient = RecipientId::known("unauthorized-client");
    let share = catalog.get_share("private_share_1", &recipient).await;
    assert!(share.is_err());

    let recipient = RecipientId::known("recipient1");
    let share = catalog.get_share("private_share_1", &recipient).await;
    assert!(share.is_ok())
}

async fn get_table<C: Catalog>(catalog: &C) {
    let recipient = RecipientId::anonymous();
    let table = catalog
        .get_table("public_share_1", "schema1", "table1", &recipient)
        .await
        .unwrap();
    assert_eq!(table.share_name(), "public_share_1");
    assert_eq!(table.schema_name(), "schema1");
    assert_eq!(table.name(), "table1");
    assert_eq!(
        table.storage_path(),
        "s3a://<bucket-name>/<table-prefix-1>/"
    );
    assert_eq!(table.get_extension("?"), None);

    let table = catalog
        .get_table("public_share_1", "schema1", "does-not-exist", &recipient)
        .await;
    assert!(table.is_err());
    assert_eq!(
        table.unwrap_err().kind(),
        CatalogErrorKind::ResourceNotFound
    );
}
