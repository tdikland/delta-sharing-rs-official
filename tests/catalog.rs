use std::collections::HashSet;

use delta_sharing::{
    auth::RecipientId,
    catalog::{
        file::{FileCatalog, FileCatalogConfig, FileFormat},
        Catalog, Pagination,
    },
};

#[tokio::test]
async fn yaml_file_catalog() {
    let cfg = FileCatalogConfig::new("./tests/resources/yaml_catalog_file.yaml");
    let catalog = FileCatalog::new(cfg);

    list_shares(&catalog).await;
    list_shares_pagination(&catalog).await;
    list_shares_acl(&catalog).await;
}

#[tokio::test]
async fn json_file_catalog() {
    let cfg = FileCatalogConfig::new("./tests/resources/json_catalog_file.json")
        .with_format(FileFormat::Json);
    let catalog = FileCatalog::new(cfg);

    list_shares(&catalog).await;
    list_shares_pagination(&catalog).await;
    list_shares_acl(&catalog).await;
}

#[tokio::test]
async fn toml_file_catalog() {
    let cfg = FileCatalogConfig::new("./tests/resources/toml_catalog_file.toml")
        .with_format(FileFormat::Toml);
    let catalog = FileCatalog::new(cfg);

    list_shares(&catalog).await;
    list_shares_pagination(&catalog).await;
    list_shares_acl(&catalog).await;
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
