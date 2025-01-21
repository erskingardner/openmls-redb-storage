pub mod helpers;
pub mod traits;

use openmls_traits::storage::*;
#[allow(unused)]
use redb::ReadableTable;
use std::path::Path;
pub struct RedbStorage {
    db: redb::Database,
}

const TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("openmls_redb_storage");

/// Errors thrown by the key store.
#[derive(thiserror::Error, Debug)]
pub enum RedbStorageError {
    #[error("Error: {0}")]
    RedbError(#[from] redb::Error),
    #[error("Database error: {0}")]
    RedbDatabaseError(#[from] redb::DatabaseError),
    #[error("Transaction error: {0}")]
    RedbTransactionError(#[from] redb::TransactionError),
    #[error("Commit error: {0}")]
    RedbCommitError(#[from] redb::CommitError),
    #[error("Table error: {0}")]
    RedbTableError(#[from] redb::TableError),
    #[error("Storage error: {0}")]
    RedbStorageError(#[from] redb::StorageError),
    #[error("Serialization error")]
    SerializationError,
    #[error("Value does not exist.")]
    None,
}

impl From<serde_json::Error> for RedbStorageError {
    fn from(_: serde_json::Error) -> Self {
        Self::SerializationError
    }
}

impl RedbStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, RedbStorageError> {
        let db = redb::Database::create(path)?;
        let wtxn = db.begin_write()?;
        // Ensure that the table exists
        wtxn.open_table(TABLE)?;
        wtxn.commit()?;
        Ok(Self { db })
    }

    pub fn delete_all_data(&self) -> Result<(), RedbStorageError> {
        let wtxn = self.db.begin_write()?;
        wtxn.delete_table(TABLE)?;
        // Recreate the table
        wtxn.open_table(TABLE)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Writes a value to the storage with the given prefix, key, and value.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    /// * `value` - The value to be stored.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a RedbStorageError.
    #[inline(always)]
    fn write<const VERSION: u16>(
        &self,
        prefix: &[u8],
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let wtxn = self.db.begin_write()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        {
            let mut table = wtxn.open_table(TABLE)?;
            table.insert(compound_key.as_slice(), value.as_slice())?;
        }
        wtxn.commit()?;
        Ok(())
    }

    /// Appends a value to a list stored at the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    /// * `value` - The value to be appended.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a RedbStorageError.
    fn append<const VERSION: u16>(
        &self,
        prefix: &[u8],
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let rtxn = self.db.begin_read()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        let table = rtxn.open_table(TABLE)?;
        let mut list: Vec<Vec<u8>> =
            if let Some(list_bytes) = table.get(&compound_key.as_slice())? {
                serde_json::from_slice(list_bytes.value())?
            } else {
                Vec::new()
            };

        list.push(value);
        let updated_list_bytes = serde_json::to_vec(&list)?;
        let wtxn = self.db.begin_write()?;
        {
            let mut table = wtxn.open_table(TABLE)?;
            table.insert(compound_key.as_slice(), updated_list_bytes.as_slice())?;
        }
        wtxn.commit()?;
        Ok(())
    }

    /// Reads a value from the storage with the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    /// * `V` - The type of the entity to be read, which must implement the `Entity<VERSION>` trait.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option` with the value (if found) or a `RedbStorageError`.
    #[inline(always)]
    fn read<const VERSION: u16, V: Entity<VERSION>>(
        &self,
        prefix: &[u8],
        key: &[u8],
    ) -> Result<Option<V>, <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let rtxn = self.db.begin_read()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        let table = rtxn.open_table(TABLE)?;
        match table.get(&compound_key.as_slice()) {
            Ok(None) => Ok(None),
            Ok(Some(value)) => {
                // Deserialize directly to V, without the intermediate Vec<u8>
                Ok(Some(serde_json::from_slice(value.value())?))
            }
            Err(e) => Err(RedbStorageError::RedbError(e.into())),
        }
    }

    /// Reads a list of entities from the storage with the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    /// * `V` - The type of the entity to be read, which must implement the `Entity<VERSION>` trait.
    ///
    /// # Returns
    ///
    /// A Result containing a Vec of entities or a RedbStorageError.
    #[inline(always)]
    fn read_list<const VERSION: u16, V: Entity<VERSION>>(
        &self,
        prefix: &[u8],
        key: &[u8],
    ) -> Result<Vec<V>, <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let rtxn = self.db.begin_read()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        let table = rtxn.open_table(TABLE)?;
        let value: Vec<Vec<u8>> = match table.get(&compound_key.as_slice()) {
            Ok(Some(list_bytes)) => serde_json::from_slice(list_bytes.value())?,
            Ok(None) => vec![],
            Err(e) => return Err(RedbStorageError::RedbError(e.into())),
        };

        value
            .iter()
            .map(|value_bytes| serde_json::from_slice(value_bytes))
            .collect::<Result<Vec<V>, _>>()
            .map_err(|_| RedbStorageError::SerializationError)
    }

    /// Removes a specific item from a list stored at the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    /// * `value` - The value to be removed.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a RedbStorageError.
    fn remove_item<const VERSION: u16>(
        &self,
        prefix: &[u8],
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let rtxn = self.db.begin_read()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        let table = rtxn.open_table(TABLE)?;
        let list = match table.get(&compound_key.as_slice()) {
            Ok(Some(list)) => list,
            Ok(None) => return Ok(()),
            Err(e) => return Err(RedbStorageError::RedbError(e.into())),
        };

        let mut parsed_list: Vec<Vec<u8>> = serde_json::from_slice(list.value())?;
        if let Some(pos) = parsed_list
            .iter()
            .position(|stored_item| stored_item == &value)
        {
            parsed_list.remove(pos);
        }

        let updated_list_bytes = serde_json::to_vec(&parsed_list)?;
        let wtxn = self.db.begin_write()?;
        {
            let mut table = wtxn.open_table(TABLE)?;
            table.insert(compound_key.as_slice(), updated_list_bytes.as_slice())?;
        }
        wtxn.commit()?;
        Ok(())
    }

    /// Deletes an entry from the storage with the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a RedbStorageError.
    #[inline(always)]
    fn delete<const VERSION: u16>(
        &self,
        prefix: &[u8],
        key: &[u8],
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let wtxn = self.db.begin_write()?;
        {
            let mut table = wtxn.open_table(TABLE)?;
            let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
            table.remove(&compound_key.as_slice())?;
        }
        wtxn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const CURRENT_VERSION: u16 = 1; // Assuming CURRENT_VERSION is 1, adjust if needed

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct TestEntity {
        data: String,
    }

    impl Entity<CURRENT_VERSION> for TestEntity {}

    fn setup_storage() -> RedbStorage {
        let dir = tempdir().unwrap();
        RedbStorage::new(dir.path().join("test.redb")).unwrap()
    }

    #[test]
    fn test_new() {
        let dir = tempdir().unwrap();
        let storage = RedbStorage::new(dir.path().join("test.redb")).unwrap();
        let rtxn = storage.db.begin_read().unwrap();
        let table = rtxn.open_table(TABLE).unwrap();
        assert!(table.first().unwrap().is_none());
    }

    #[test]
    fn test_write_and_read() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let value = TestEntity {
            data: "test_data".to_string(),
        };

        let serialized = serde_json::to_vec(&value).unwrap();
        println!(
            "Serialized value: {:?}",
            String::from_utf8_lossy(&serialized)
        );

        let write_result = storage.write::<CURRENT_VERSION>(prefix, key, serialized);
        assert!(write_result.is_ok());

        let read_result: Result<Option<TestEntity>, _> =
            storage.read::<CURRENT_VERSION, _>(prefix, key);

        // Debug the actual value we got back
        match &read_result {
            Ok(Some(v)) => println!("Read value: {:?}", v),
            Ok(None) => println!("Got None!"),
            Err(e) => println!("Got error: {:?}", e),
        }

        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), Some(value));
    }

    #[test]
    fn test_append_and_read_list() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let values = vec![
            TestEntity {
                data: "data1".to_string(),
            },
            TestEntity {
                data: "data2".to_string(),
            },
        ];

        for value in &values {
            let append_result =
                storage.append::<CURRENT_VERSION>(prefix, key, serde_json::to_vec(value).unwrap());
            assert!(append_result.is_ok());
        }

        let read_result: Result<Vec<TestEntity>, _> =
            storage.read_list::<CURRENT_VERSION, _>(prefix, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), values);
    }

    #[test]
    fn test_remove_item() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let values = vec![
            TestEntity {
                data: "data1".to_string(),
            },
            TestEntity {
                data: "data2".to_string(),
            },
        ];

        for value in &values {
            storage
                .append::<CURRENT_VERSION>(prefix, key, serde_json::to_vec(value).unwrap())
                .unwrap();
        }

        let remove_result = storage.remove_item::<CURRENT_VERSION>(
            prefix,
            key,
            serde_json::to_vec(&values[0]).unwrap(),
        );
        assert!(remove_result.is_ok());

        let read_result: Result<Vec<TestEntity>, _> =
            storage.read_list::<CURRENT_VERSION, _>(prefix, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), vec![values[1].clone()]);
    }

    #[test]
    fn test_delete() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let value = TestEntity {
            data: "test_data".to_string(),
        };

        storage
            .write::<CURRENT_VERSION>(prefix, key, serde_json::to_vec(&value).unwrap())
            .unwrap();

        let delete_result = storage.delete::<CURRENT_VERSION>(prefix, key);
        assert!(delete_result.is_ok());

        let read_result: Result<Option<TestEntity>, _> =
            storage.read::<CURRENT_VERSION, _>(prefix, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), None);
    }

    #[test]
    fn test_delete_all_data() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let value = TestEntity {
            data: "test_data".to_string(),
        };

        // Write some data
        storage
            .write::<CURRENT_VERSION>(prefix, key, serde_json::to_vec(&value).unwrap())
            .unwrap();

        // Delete all data
        let delete_result = storage.delete_all_data();
        assert!(delete_result.is_ok());

        // Try to read the data back
        let read_result: Result<Option<TestEntity>, _> =
            storage.read::<CURRENT_VERSION, _>(prefix, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), None);
    }

    #[test]
    fn test_read_nonexistent_key() {
        let storage = setup_storage();
        let prefix = b"nonexistent_prefix";
        let key = b"nonexistent_key";

        let read_result: Result<Option<TestEntity>, _> =
            storage.read::<CURRENT_VERSION, _>(prefix, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), None);
    }

    #[test]
    fn test_remove_from_empty_list() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let value = TestEntity {
            data: "test_data".to_string(),
        };

        // Try to remove from non-existent list
        let remove_result = storage.remove_item::<CURRENT_VERSION>(
            prefix,
            key,
            serde_json::to_vec(&value).unwrap(),
        );
        assert!(remove_result.is_ok());
    }

    #[test]
    fn test_read_empty_list() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";

        let read_result: Result<Vec<TestEntity>, _> =
            storage.read_list::<CURRENT_VERSION, _>(prefix, key);
        assert!(read_result.is_ok());
        assert!(read_result.unwrap().is_empty());
    }
}
