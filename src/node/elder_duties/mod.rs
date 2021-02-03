// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod data_section;
mod key_section;

use self::{data_section::DataSection, key_section::KeySection};
use crate::{
    capacity::{Capacity, ChunkHolderDbs, RateLimit},
    node::node_ops::{ElderDuty, NodeOperation},
    ElderState, Result,
};
use log::trace;
use sn_data_types::{
    Map, MapAddress, PublicKey, Sequence, SequenceAddress, TransferPropagated, WalletInfo,
};
use sn_routing::Prefix;
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use xor_name::XorName;

/// Duties carried out by an Elder node.
pub struct ElderDuties {
    state: ElderState,
    key_section: KeySection,
    data_section: DataSection,
}

impl ElderDuties {
    pub async fn new(wallet_info: WalletInfo, state: ElderState) -> Result<Self> {
        let info = state.info();
        let dbs = ChunkHolderDbs::new(info.path(), info.init_mode)?;
        let rate_limit = RateLimit::new(state.clone(), Capacity::new(dbs.clone()));
        let key_section = KeySection::new(rate_limit, state.clone()).await?;
        let data_section = DataSection::new(info, dbs, wallet_info, state.clone()).await?;
        Ok(Self {
            state,
            key_section,
            data_section,
        })
    }

    ///
    pub fn state(&self) -> &ElderState {
        &self.state
    }

    /// Issues queries to Elders of the section
    /// as to catch up with shares state and
    /// start working properly in the group.
    pub async fn initiate(&mut self, genesis: Option<TransferPropagated>) -> Result<NodeOperation> {
        let mut ops = vec![];
        if let Some(genesis) = genesis {
            // if we are genesis
            // does local init, with no roundrip via network messaging
            let _ = self.key_section.init_genesis_node(genesis).await?;
        } else {
            ops.push(self.key_section.catchup_with_section().await?);
            ops.push(self.data_section.catchup_with_section().await?);
        }

        Ok(ops.into())
    }

    /// Processing of any Elder duty.
    pub async fn process_elder_duty(&mut self, duty: ElderDuty) -> Result<NodeOperation> {
        trace!("Processing elder duty: {:?}", duty);
        use ElderDuty::*;
        match duty {
            ProcessNewMember(name) => self.new_node_joined(name).await,
            ProcessLostMember { name, age } => self.member_left(name, age).await,
            ProcessRelocatedMember {
                old_node_id,
                new_node_id,
                age,
            } => {
                self.relocated_node_joined(old_node_id, new_node_id, age)
                    .await
            }
            RunAsKeySection(the_key_duty) => {
                self.key_section
                    .process_key_section_duty(the_key_duty)
                    .await
            }
            RunAsDataSection(duty) => self.data_section.process_data_section_duty(duty).await,
            StorageFull { node_id } => self.increase_full_node_count(node_id).await,
            SwitchNodeJoin(joins_allowed) => {
                self.key_section.set_node_join_flag(joins_allowed).await
            }
            PrepareForUpdateDataResponse {
                requester,
                correlation_id,
            } => {
                self.data_section
                    .prepare_to_give_data(requester, correlation_id)
                    .await
            }
            UpdateDataStores { data } => self.data_section.update_data_store(data).await,
            NoOp => Ok(NodeOperation::NoOp),
        }
    }

    ///
    async fn new_node_joined(&mut self, name: XorName) -> Result<NodeOperation> {
        self.data_section.new_node_joined(name).await
    }

    async fn increase_full_node_count(&mut self, node_id: PublicKey) -> Result<NodeOperation> {
        self.key_section
            .increase_full_node_count(node_id)
            .await
            .map(|()| NodeOperation::NoOp)
    }

    ///
    async fn relocated_node_joined(
        &mut self,
        old_node_id: XorName,
        new_node_id: XorName,
        age: u8,
    ) -> Result<NodeOperation> {
        self.data_section
            .relocated_node_joined(old_node_id, new_node_id, age)
            .await
    }

    ///
    async fn member_left(&mut self, node_id: XorName, age: u8) -> Result<NodeOperation> {
        self.data_section.member_left(node_id, age).await
    }

    ///
    pub async fn initiate_elder_change(
        &mut self,
        elder_state: ElderState,
    ) -> Result<NodeOperation> {
        // 1. First we must update data section..
        self.data_section.initiate_elder_change(elder_state).await
    }

    ///
    pub async fn finish_elder_change(&mut self, state: ElderState) -> Result<()> {
        // 2. Then we must update key section..
        let info = state.info();
        let dbs = ChunkHolderDbs::new(info.path(), crate::utils::Init::Load)?;
        let rate_limit = RateLimit::new(state.clone(), Capacity::new(dbs));
        self.key_section.elders_changed(state, rate_limit);
        Ok(())
    }

    ///
    pub async fn split_section(&mut self, prefix: Prefix) -> Result<NodeOperation> {
        let _ = self.key_section.split_section(prefix).await?;
        self.data_section.split_section(prefix).await
    }
}

impl Display for ElderDuties {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "ElderDuties")
    }
}

use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
pub(crate) struct BlobDataExchange {
    /// Full Adults register
    pub full_adults: BTreeMap<String, String>,
    /// Blob holders register
    pub holders: BTreeMap<String, String>,
    /// Metadata register
    pub metadata: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct MapDataExchange(BTreeMap<MapAddress, Map>);

#[derive(Serialize, Deserialize)]
pub(crate) struct SequenceDataExchange(BTreeMap<SequenceAddress, Sequence>);
