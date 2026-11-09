use crate::types::{AreaPosition, UniqueGift};
use serde::{Deserialize, Serialize};

/// Describes a service message about a transfer with a TON wallet.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TonWalletTransfer {
    /// Unique identifier of the transaction; may be empty if the transfer
    /// hasn't been sent yet
    pub transaction_id: String,
    /// Address of the transfer peer in the TON blockchain
    pub peer_address: String,
    /// The amount of transferred cryptocurrency, in the smallest units of the
    /// cryptocurrency, as a decimal number; negative for outgoing transfers
    pub amount: String,
    /// Comment added to the transfer; omitted if the comment is encrypted
    pub comment: Option<String>,
}

/// This object represents an NFT in the TON blockchain.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TonNft {
    /// Address of the NFT
    pub address: String,
    /// Address of the collection to which the NFT belongs; omitted if unknown
    pub collection_address: Option<String>,
    /// Index of the NFT among all NFTs from the collection as a decimal number
    pub index: String,
    /// Name of the NFT
    pub name: String,
    /// Description of the NFT
    pub description: String,
    /// Attributes of the NFT
    pub attributes: Option<Vec<TonNftAttribute>>,
    /// The unique gift corresponding to the NFT
    pub gift: Option<UniqueGift>,
}

/// This object represents an attribute of an NFT in the TON blockchain.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TonNftAttribute {
    /// Trait type of the attribute
    pub trait_type: String,
    /// Value of the attribute
    pub value: String,
}

/// This object describes a sticker on a TON wallet card.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TonWalletCardSticker {
    /// Position of the sticker on the card
    pub position: AreaPosition,
    /// The NFT shown as the sticker
    pub nft: TonNft,
}

/// This object describes a TON wallet card, which is shown on the profile page
/// of a user to everyone.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TonWalletCard {
    /// Address of the wallet
    pub address: String,
    /// Balance of the wallet, in the smallest units of the cryptocurrency, as a
    /// decimal number
    pub amount: String,
    /// Stickers placed on the card
    pub stickers: Vec<TonWalletCardSticker>,
    /// Address of the NFT of one of the stickers, whose colors must be used for
    /// the card background; omitted if the default background must be used
    pub background_nft_address: Option<String>,
}
