// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    blob_register::BlobRegister, map_storage::MapStorage, sequence_storage::SequenceStorage,
};
use crate::node::elder_duties::{BlobDataExchange, MapDataExchange, SequenceDataExchange};
use crate::Result;
use log::info;
use std::collections::BTreeMap;

/// The various data type stores,
/// that are only managed at Elders.
pub(super) struct ElderStores {
    blob_register: BlobRegister,
    map_storage: MapStorage,
    sequence_storage: SequenceStorage,
}

impl ElderStores {
    pub fn new(
        blob_register: BlobRegister,
        map_storage: MapStorage,
        sequence_storage: SequenceStorage,
    ) -> Self {
        Self {
            blob_register,
            map_storage,
            sequence_storage,
        }
    }

    pub fn blob_register(&self) -> &BlobRegister {
        &self.blob_register
    }

    pub fn map_storage(&self) -> &MapStorage {
        &self.map_storage
    }

    pub fn sequence_storage(&self) -> &SequenceStorage {
        &self.sequence_storage
    }

    pub fn blob_register_mut(&mut self) -> &mut BlobRegister {
        &mut self.blob_register
    }

    pub fn map_storage_mut(&mut self) -> &mut MapStorage {
        &mut self.map_storage
    }

    pub fn sequence_storage_mut(&mut self) -> &mut SequenceStorage {
        &mut self.sequence_storage
    }

    pub fn fetch_all_data_for(&self) -> Result<BTreeMap<String, Vec<u8>>> {
        let blob_register = self.blob_register.fetch_register()?;
        let map_list = self.map_storage.fetch_data()?;
        let sequence_list = self.sequence_storage.fetch_data()?;

        let mut aggregated_map = BTreeMap::new();
        let _ = aggregated_map.insert(
            "BlobRegister".to_string(),
            bincode::serialize(&blob_register)?,
        );
        let _ = aggregated_map.insert("MapData".to_string(), bincode::serialize(&map_list)?);
        let _ = aggregated_map.insert(
            "SequenceData".to_string(),
            bincode::serialize(&sequence_list)?,
        );

        Ok(aggregated_map)
    }

    pub async fn catchup_with_section(
        &mut self,
        blob_data_exchange: BlobDataExchange,
        map_data_exchange: MapDataExchange,
        seq_data_exchange: SequenceDataExchange,
    ) -> Result<()> {
        info!("Updating ChunkStores and BlobRegister");
        self.blob_register
            .catchup_with_section(blob_data_exchange)?;
        self.map_storage
            .catchup_with_section(map_data_exchange)
            .await?;
        self.sequence_storage
            .catchup_with_section(seq_data_exchange)
            .await?;
        info!("Successfully updated ChunkStores");
        Ok(())
    }
}
