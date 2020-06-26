// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use safe_nd::{
    AccountId, DebitAgreementProof, Error, KnownGroupAdded, Money, ReplicaEvent, Result,
    SignedTransfer, TransferPropagated, TransferRegistered, TransferValidated,
};
use safe_transfers::TransferReplica as Replica;
use std::collections::HashMap;
use threshold_crypto::{PublicKeySet, SecretKeyShare};

#[cfg(feature = "simulated-payouts")]
use {
    crate::{action::Action, rpc::Rpc},
    safe_nd::{
        MessageId, PublicId, PublicKey, Response, Signature, SignatureShare, Transfer, XorName,
    },
    threshold_crypto::SecretKey,
};

pub(super) struct ReplicaManager {
    store: EventStore,
    replica: Replica,
}

#[allow(unused)]
impl ReplicaManager {
    pub fn new(
        secret_key: &SecretKeyShare,
        key_index: usize,
        peer_replicas: &PublicKeySet,
        events: Vec<ReplicaEvent>,
    ) -> Result<Self> {
        let mut store = EventStore {
            streams: Default::default(),
            group_changes: Default::default(),
        };
        /// OKs on empty vec as well, only errors from underlying storage.
        match store.init(events.clone()) {
            Ok(()) => {
                let mut replica = Replica::from_history(
                    secret_key.clone(),
                    key_index,
                    peer_replicas.clone(),
                    events,
                );
                Ok(Self { store, replica })
            }
            Err(e) => Err(Error::InvalidOperation), // todo: storage error
        }
    }

    pub fn history(&self, id: &AccountId) -> Option<&Vec<ReplicaEvent>> {
        self.store.history(id)
    }

    pub fn balance(&self, id: &AccountId) -> Option<Money> {
        self.replica.balance(id)
    }

    fn churn(
        &mut self,
        secret_key: SecretKeyShare,
        index: usize,
        peer_replicas: PublicKeySet,
    ) -> Result<()> {
        match self.store.try_load() {
            Ok(events) => {
                self.replica = Replica::from_history(secret_key, index, peer_replicas, events);
                Ok(())
            }
            Err(e) => Err(Error::InvalidOperation), // todo: storage error
        }
    }

    pub fn validate(&mut self, transfer: SignedTransfer) -> Result<TransferValidated> {
        let event = self.replica.validate(transfer)?;
        match self.persist(ReplicaEvent::TransferValidated(event.clone())) {
            Ok(()) => Ok(event),
            Err(err) => Err(err),
        }
    }

    pub fn register(&mut self, proof: &DebitAgreementProof) -> Result<TransferRegistered> {
        let event = self.replica.register(proof)?;
        match self.persist(ReplicaEvent::TransferRegistered(event.clone())) {
            Ok(()) => Ok(event),
            Err(err) => Err(err),
        }
    }

    pub fn receive_propagated(
        &mut self,
        proof: &DebitAgreementProof,
    ) -> Result<TransferPropagated> {
        let event = self.replica.receive_propagated(proof)?;
        match self.persist(ReplicaEvent::TransferPropagated(event.clone())) {
            Ok(()) => Ok(event),
            Err(err) => Err(err),
        }
    }

    fn persist(&mut self, event: ReplicaEvent) -> Result<()> {
        self.store.try_append(event.clone())?;
        self.replica.apply(event);
        Ok(())
    }

    /// Get the replica's PK set
    pub fn replicas_pk_set(&self) -> Result<PublicKeySet> {
        self.replica.replicas_pk_set()
    }
}

#[cfg(feature = "simulated-payouts")]
impl ReplicaManager {
    pub fn credit_without_proof(
        &mut self,
        requester: PublicId,
        transfer: Transfer,
        message_id: MessageId,
        sender: XorName,
    ) -> Option<Action> {
        self.replica.credit_without_proof(transfer.clone());
        let dummy_msg = "DUMMY MSG";
        let sec_key = SecretKey::random();
        let pub_key = sec_key.public_key();
        let dummy_shares = SecretKeyShare::default();
        let dummy_sig = dummy_shares.sign(dummy_msg);
        let sig = sec_key.sign(dummy_msg);
        let debit_proof = DebitAgreementProof {
            signed_transfer: SignedTransfer {
                transfer,
                actor_signature: Signature::from(sig.clone()),
            },
            debiting_replicas_sig: Signature::from(sig),
        };
        self.store
            .try_append(ReplicaEvent::TransferPropagated(TransferPropagated {
                debit_proof: debit_proof.clone(),
                debiting_replicas: PublicKey::from(pub_key),
                crediting_replica_sig: SignatureShare {
                    index: 0,
                    share: dummy_sig,
                },
            }))
            .ok()?;
        // Respond to the Client with TransferRegistered
        Some(Action::RespondToClientHandlers {
            sender,
            rpc: Rpc::Response {
                response: Response::TransferRegistration(Ok(TransferRegistered { debit_proof })),
                requester,
                message_id,
                refund: None,
            },
        })
    }

    pub fn debit_without_proof(&mut self, transfer: Transfer) {
        self.replica.debit_without_proof(transfer)
    }
}

/// Disk storage
struct EventStore {
    streams: HashMap<AccountId, Vec<ReplicaEvent>>,
    group_changes: Vec<KnownGroupAdded>,
}

/// In memory store lacks transactionality
impl EventStore {
    fn history(&self, id: &AccountId) -> Option<&Vec<ReplicaEvent>> {
        self.streams.get(id)
    }

    fn try_load(&self) -> Result<Vec<ReplicaEvent>> {
        // Only the order within the streams is important, not between streams.
        Ok(self
            .streams
            .values()
            .cloned()
            .flatten()
            .collect::<Vec<ReplicaEvent>>())
    }

    fn init(&mut self, events: Vec<ReplicaEvent>) -> Result<()> {
        for event in events {
            self.try_append(event)?;
        }
        Ok(())
    }

    fn try_append(&mut self, event: ReplicaEvent) -> Result<()> {
        match event.clone() {
            ReplicaEvent::KnownGroupAdded(e) => {
                self.group_changes.push(e);
            }
            ReplicaEvent::TransferPropagated(e) => {
                let id = e.debit_proof.signed_transfer.transfer.to;
                match self.streams.get_mut(&id) {
                    Some(stream) => stream.push(event),
                    None => {
                        // Creates if not exists. A stream always starts with a credit.
                        let _ = self.streams.insert(id, vec![event]);
                    }
                }
            }
            ReplicaEvent::TransferValidated(e) => {
                let id = e.signed_transfer.transfer.id.actor;
                match self.streams.get_mut(&id) {
                    Some(stream) => stream.push(event),
                    None => return Err(Error::InvalidOperation), // A stream cannot start with a debit.
                }
            }
            ReplicaEvent::TransferRegistered(e) => {
                let id = e.debit_proof.signed_transfer.transfer.to;
                match self.streams.get_mut(&id) {
                    Some(stream) => stream.push(event),
                    None => return Err(Error::InvalidOperation), // A stream cannot start with a debit.
                }
            }
        };
        Ok(())
    }
}
