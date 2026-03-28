#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMigrationPlan {
    pub from_version: u32,
    pub to_version: u32,
    pub requires_import_export: bool,
    pub steps: Vec<&'static str>,
}

pub fn plan_catalog_migration(from_version: u32) -> Result<StorageMigrationPlan, String> {
    if from_version > crate::STORAGE_FORMAT_VERSION {
        return Err(format!(
            "Catalog format version {from_version} is newer than supported version {}",
            crate::STORAGE_FORMAT_VERSION
        ));
    }

    if from_version == crate::STORAGE_FORMAT_VERSION {
        return Ok(StorageMigrationPlan {
            from_version,
            to_version: crate::STORAGE_FORMAT_VERSION,
            requires_import_export: false,
            steps: vec!["direct-open"],
        });
    }

    Ok(StorageMigrationPlan {
        from_version,
        to_version: crate::STORAGE_FORMAT_VERSION,
        requires_import_export: true,
        steps: vec!["export-import"],
    })
}
