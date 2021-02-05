// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    chunk_store::BlobChunkStore,
    error::convert_to_error_message,
    node::{
        msg_wrapping::AdultMsgWrapping,
        node_ops::{Msg, NodeMessagingDuty},
        Error,
    },
    AdultState, NodeInfo, Result,
};
use log::{error, info};
use sn_data_types::{Blob, BlobAddress, Signature};
use sn_messaging::client::{
    Address, AdultDuties, CmdError, Error as ErrorMessage, Message, MessageId, NodeCmdError,
    NodeDataError, NodeDataQuery, NodeDataQueryResponse, NodeEvent, NodeQuery, NodeQueryResponse,
    QueryResponse,
};
use sn_routing::DstLocation;
use std::{
    collections::BTreeSet,
    fmt::{self, Display, Formatter},
};
use xor_name::XorName;

/// Storage of data chunks.
pub(crate) struct ChunkStorage {
    chunks: BlobChunkStore,
    wrapping: AdultMsgWrapping,
}

impl ChunkStorage {
    pub(crate) async fn new(node_info: &NodeInfo, adult_state: AdultState) -> Result<Self> {
        let chunks = BlobChunkStore::new(&node_info.root_dir, node_info.used_space.clone()).await?;
        let wrapping = AdultMsgWrapping::new(adult_state, AdultDuties::ChunkStorage);
        Ok(Self { chunks, wrapping })
    }

    pub(crate) async fn store(
        &mut self,
        data: &Blob,
        msg_id: MessageId,
        origin: XorName,
    ) -> Result<NodeMessagingDuty> {
        if let Err(error) = self.try_store(data, origin).await {
            let message_error = convert_to_error_message(error)?;

            return self
                .wrapping
                .error(CmdError::Data(message_error), msg_id, origin)
                .await;
        }
        Ok(NodeMessagingDuty::NoOp)
    }

    #[allow(unused)]
    pub(crate) async fn take_replica(
        &mut self,
        data: &Blob,
        msg_id: MessageId,
        origin: XorName,
        accumulated_signature: &Signature,
    ) -> Result<NodeMessagingDuty> {
        let msg = match self.try_store(data, origin).await {
            Ok(()) => Message::NodeEvent {
                event: NodeEvent::ReplicationCompleted {
                    chunk: *data.address(),
                    proof: accumulated_signature.clone(),
                },
                id: MessageId::new(),
                correlation_id: msg_id,
            },
            Err(error) => {
                let message_error = convert_to_error_message(error)?;
                Message::NodeCmdError {
                    id: MessageId::new(),
                    error: NodeCmdError::Data(NodeDataError::ChunkReplication {
                        address: *data.address(),
                        error: message_error,
                    }),
                    correlation_id: msg_id,
                    cmd_origin: Address::Node(origin),
                }
            }
        };
        self.wrapping
            .send_to_node(Msg {
                msg,
                dst: DstLocation::Node(origin),
            })
            .await
    }

    async fn try_store(&mut self, data: &Blob, origin: XorName) -> Result<()> {
        info!("TRYING TO STORE BLOB");
        if data.is_unpub() {
            let data_owner = *data.owner().ok_or(Error::InvalidOperation)?; // Error::InvalidOwners(origin)
            let owner: XorName = data_owner.into();
            info!("Blob is unpub");
            info!("DATA OWNER: {:?}", owner);
            info!("ORIGIN: {:?}", origin);
            if owner != origin {
                info!("INVALID OWNER! Returning error");
                return Err(Error::InvalidOwners(data_owner)); // should be origin here..
            }
        }

        if self.chunks.has(data.address()) {
            info!(
                "{}: Immutable chunk already exists, not storing: {:?}",
                self,
                data.address()
            );
            return Err(Error::DataExists);
        }
        self.chunks.put(&data).await
    }

    pub(crate) async fn get(
        &self,
        address: &BlobAddress,
        msg_id: MessageId,
        origin: XorName,
    ) -> Result<NodeMessagingDuty> {
        let result = self
            .chunks
            .get(address)
            .map_err(|_| ErrorMessage::NoSuchData);
        self.wrapping
            .send_to_section(
                Msg {
                    msg: Message::QueryResponse {
                        id: MessageId::in_response_to(&msg_id),
                        response: QueryResponse::GetBlob(result),
                        correlation_id: msg_id,
                        query_origin: Address::Client(origin),
                    },
                    dst: DstLocation::Client(origin),
                },
                true,
            )
            .await
    }

    pub async fn replicate_chunk(
        &self,
        address: BlobAddress,
        current_holders: BTreeSet<XorName>,
        //section_authority: MsgSender,
        //_msg_id: MessageId,
        //_origin: MsgSender,
    ) -> Result<NodeMessagingDuty> {
        let message = Message::NodeQuery {
            query: NodeQuery::Data(NodeDataQuery::GetChunk {
                //section_authority,
                address,
                new_holder: self.wrapping.name(),
                current_holders: current_holders.clone(),
            }),
            id: MessageId::new(),
        };
        info!("Sending NodeDataQuery::GetChunk to existing holders");
        self.wrapping
            .send_to_adults(message, current_holders, AdultDuties::ChunkReplication)
            .await
    }

    ///
    pub async fn get_for_replication(
        &self,
        address: BlobAddress,
        msg_id: MessageId,
        origin: XorName,
    ) -> Result<NodeMessagingDuty> {
        let result = match self.chunks.get(&address) {
            Ok(res) => Ok(res),
            Err(error) => Err(convert_to_error_message(error)?),
        };

        self.wrapping
            .send_to_node(Msg {
                msg: Message::NodeQueryResponse {
                    response: NodeQueryResponse::Data(NodeDataQueryResponse::GetChunk(result)),
                    id: MessageId::new(),
                    correlation_id: msg_id,
                    query_origin: Address::Node(origin),
                },
                dst: DstLocation::Node(origin),
            })
            .await
    }

    ///
    pub async fn store_for_replication(&mut self, blob: Blob) -> Result<NodeMessagingDuty> {
        if self.chunks.has(blob.address()) {
            info!(
                "{}: Immutable chunk already exists, not storing: {:?}",
                self,
                blob.address()
            );
            return Ok(NodeMessagingDuty::NoOp);
        }

        self.chunks.put(&blob).await?;

        Ok(NodeMessagingDuty::NoOp)
    }

    pub async fn used_space_ratio(&self) -> f64 {
        self.chunks.used_space_ratio().await
    }

    // pub(crate) fn get_for_duplciation(
    //     &self,
    //     address: BlobAddress,
    //     msg: &Message,
    // ) -> Result<NodeMessagingDuty> {

    //     match self.chunks.get(&address) {

    //     }

    //     let mut targets: BTreeSet<XorName> = Default::default();
    //     let _ = targets.insert(XorName(xorname.0));
    //     Some(NodeMessagingDuty::SendToNode {
    //         targets,
    //         msg: Message::QueryResponse {
    //             requester: requester.clone(),
    //             response: Response::GetBlob(result),
    //             message_id,
    //             proof: Some((request, (accumulated_signature?).clone())),
    //         },
    //     })
    // }

    pub(crate) async fn delete(
        &mut self,
        address: BlobAddress,
        msg_id: MessageId,
        origin: XorName,
    ) -> Result<NodeMessagingDuty> {
        if !self.chunks.has(&address) {
            info!("{}: Immutable chunk doesn't exist: {:?}", self, address);
            return Ok(NodeMessagingDuty::NoOp);
        }

        let result = match self.chunks.get(&address) {
            Ok(Blob::Private(data)) => {
                let data_owner: XorName = (*data.owner()).into();
                if data_owner == origin {
                    self.chunks
                        .delete(&address)
                        .await
                        .map_err(|_error| ErrorMessage::FailedToDelete)
                } else {
                    Err(ErrorMessage::InvalidOwners(*data.owner())) // should be origin...
                }
            }
            Ok(_) => {
                error!(
                    "{}: Invalid DeletePrivate(Blob::Public) encountered: {:?}",
                    self, msg_id
                );
                Err(ErrorMessage::InvalidOperation)
            }
            _ => Err(ErrorMessage::NoSuchKey),
        };

        if let Err(error) = result {
            return self
                .wrapping
                .error(CmdError::Data(error), msg_id, origin)
                .await;
        }
        Ok(NodeMessagingDuty::NoOp)
    }
}

impl Display for ChunkStorage {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "ChunkStorage")
    }
}
