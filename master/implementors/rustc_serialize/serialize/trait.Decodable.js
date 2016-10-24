(function() {var implementors = {};
implementors["bincode"] = ["impl&lt;T:&nbsp;<a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a>&gt; <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='bincode/struct.RefBox.html' title='bincode::RefBox'>RefBox</a>&lt;'static,&nbsp;T&gt;","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='bincode/struct.StrBox.html' title='bincode::StrBox'>StrBox</a>&lt;'static&gt;","impl&lt;T:&nbsp;<a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a>&gt; <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='bincode/struct.SliceBox.html' title='bincode::SliceBox'>SliceBox</a>&lt;'static,&nbsp;T&gt;",];implementors["rust_sodium"] = ["impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/box_/curve25519xsalsa20poly1305/struct.SecretKey.html' title='rust_sodium::crypto::box_::curve25519xsalsa20poly1305::SecretKey'>SecretKey</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/box_/curve25519xsalsa20poly1305/struct.PublicKey.html' title='rust_sodium::crypto::box_::curve25519xsalsa20poly1305::PublicKey'>PublicKey</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/box_/curve25519xsalsa20poly1305/struct.Nonce.html' title='rust_sodium::crypto::box_::curve25519xsalsa20poly1305::Nonce'>Nonce</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/box_/curve25519xsalsa20poly1305/struct.PrecomputedKey.html' title='rust_sodium::crypto::box_::curve25519xsalsa20poly1305::PrecomputedKey'>PrecomputedKey</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/sign/ed25519/struct.Seed.html' title='rust_sodium::crypto::sign::ed25519::Seed'>Seed</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/sign/ed25519/struct.SecretKey.html' title='rust_sodium::crypto::sign::ed25519::SecretKey'>SecretKey</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/sign/ed25519/struct.PublicKey.html' title='rust_sodium::crypto::sign::ed25519::PublicKey'>PublicKey</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/sign/ed25519/struct.Signature.html' title='rust_sodium::crypto::sign::ed25519::Signature'>Signature</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/scalarmult/curve25519/struct.Scalar.html' title='rust_sodium::crypto::scalarmult::curve25519::Scalar'>Scalar</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/scalarmult/curve25519/struct.GroupElement.html' title='rust_sodium::crypto::scalarmult::curve25519::GroupElement'>GroupElement</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/auth/hmacsha512/struct.Key.html' title='rust_sodium::crypto::auth::hmacsha512::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/auth/hmacsha512/struct.Tag.html' title='rust_sodium::crypto::auth::hmacsha512::Tag'>Tag</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/auth/hmacsha512256/struct.Key.html' title='rust_sodium::crypto::auth::hmacsha512256::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/auth/hmacsha512256/struct.Tag.html' title='rust_sodium::crypto::auth::hmacsha512256::Tag'>Tag</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/auth/hmacsha256/struct.Key.html' title='rust_sodium::crypto::auth::hmacsha256::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/auth/hmacsha256/struct.Tag.html' title='rust_sodium::crypto::auth::hmacsha256::Tag'>Tag</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/hash/sha512/struct.Digest.html' title='rust_sodium::crypto::hash::sha512::Digest'>Digest</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/hash/sha256/struct.Digest.html' title='rust_sodium::crypto::hash::sha256::Digest'>Digest</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/secretbox/xsalsa20poly1305/struct.Key.html' title='rust_sodium::crypto::secretbox::xsalsa20poly1305::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/secretbox/xsalsa20poly1305/struct.Nonce.html' title='rust_sodium::crypto::secretbox::xsalsa20poly1305::Nonce'>Nonce</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/onetimeauth/poly1305/struct.Key.html' title='rust_sodium::crypto::onetimeauth::poly1305::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/onetimeauth/poly1305/struct.Tag.html' title='rust_sodium::crypto::onetimeauth::poly1305::Tag'>Tag</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/pwhash/scryptsalsa208sha256/struct.Salt.html' title='rust_sodium::crypto::pwhash::scryptsalsa208sha256::Salt'>Salt</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/pwhash/scryptsalsa208sha256/struct.HashedPassword.html' title='rust_sodium::crypto::pwhash::scryptsalsa208sha256::HashedPassword'>HashedPassword</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/xsalsa20/struct.Key.html' title='rust_sodium::crypto::stream::xsalsa20::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/xsalsa20/struct.Nonce.html' title='rust_sodium::crypto::stream::xsalsa20::Nonce'>Nonce</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/aes128ctr/struct.Key.html' title='rust_sodium::crypto::stream::aes128ctr::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/aes128ctr/struct.Nonce.html' title='rust_sodium::crypto::stream::aes128ctr::Nonce'>Nonce</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/salsa208/struct.Key.html' title='rust_sodium::crypto::stream::salsa208::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/salsa208/struct.Nonce.html' title='rust_sodium::crypto::stream::salsa208::Nonce'>Nonce</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/salsa2012/struct.Key.html' title='rust_sodium::crypto::stream::salsa2012::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/salsa2012/struct.Nonce.html' title='rust_sodium::crypto::stream::salsa2012::Nonce'>Nonce</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/salsa20/struct.Key.html' title='rust_sodium::crypto::stream::salsa20::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/salsa20/struct.Nonce.html' title='rust_sodium::crypto::stream::salsa20::Nonce'>Nonce</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/chacha20/struct.Key.html' title='rust_sodium::crypto::stream::chacha20::Key'>Key</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/stream/chacha20/struct.Nonce.html' title='rust_sodium::crypto::stream::chacha20::Nonce'>Nonce</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/shorthash/siphash24/struct.Digest.html' title='rust_sodium::crypto::shorthash::siphash24::Digest'>Digest</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='rust_sodium/crypto/shorthash/siphash24/struct.Key.html' title='rust_sodium::crypto::shorthash::siphash24::Key'>Key</a>",];implementors["crust"] = ["impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='crust/struct.PeerId.html' title='crust::PeerId'>PeerId</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='crust/struct.PubConnectionInfo.html' title='crust::PubConnectionInfo'>PubConnectionInfo</a>",];implementors["routing"] = ["impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/enum.Authority.html' title='routing::Authority'>Authority</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/enum.Data.html' title='routing::Data'>Data</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/enum.DataIdentifier.html' title='routing::DataIdentifier'>DataIdentifier</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/struct.PublicId.html' title='routing::PublicId'>PublicId</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/struct.ImmutableData.html' title='routing::ImmutableData'>ImmutableData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/enum.Request.html' title='routing::Request'>Request</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/enum.Response.html' title='routing::Response'>Response</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/enum.Filter.html' title='routing::Filter'>Filter</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/struct.AppendedData.html' title='routing::AppendedData'>AppendedData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/enum.AppendWrapper.html' title='routing::AppendWrapper'>AppendWrapper</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/struct.PrivAppendedData.html' title='routing::PrivAppendedData'>PrivAppendedData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/struct.PrivAppendableData.html' title='routing::PrivAppendableData'>PrivAppendableData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/struct.PubAppendableData.html' title='routing::PubAppendableData'>PubAppendableData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/struct.StructuredData.html' title='routing::StructuredData'>StructuredData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/struct.MessageId.html' title='routing::MessageId'>MessageId</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/struct.XorName.html' title='routing::XorName'>XorName</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/messaging/struct.MpidHeader.html' title='routing::messaging::MpidHeader'>MpidHeader</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/messaging/struct.MpidMessage.html' title='routing::messaging::MpidMessage'>MpidMessage</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/messaging/enum.MpidMessageWrapper.html' title='routing::messaging::MpidMessageWrapper'>MpidMessageWrapper</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/client_errors/enum.GetError.html' title='routing::client_errors::GetError'>GetError</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/client_errors/enum.MutationError.html' title='routing::client_errors::MutationError'>MutationError</a>",];implementors["safe_vault"] = ["impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='safe_vault/struct.Config.html' title='safe_vault::Config'>Config</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/authority/enum.Authority.html' title='routing::authority::Authority'>Authority</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/data/enum.Data.html' title='routing::data::Data'>Data</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/data/enum.DataIdentifier.html' title='routing::data::DataIdentifier'>DataIdentifier</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/id/struct.PublicId.html' title='routing::id::PublicId'>PublicId</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/immutable_data/struct.ImmutableData.html' title='routing::immutable_data::ImmutableData'>ImmutableData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/messages/enum.Request.html' title='routing::messages::Request'>Request</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/messages/enum.Response.html' title='routing::messages::Response'>Response</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/append_types/enum.Filter.html' title='routing::append_types::Filter'>Filter</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/append_types/struct.AppendedData.html' title='routing::append_types::AppendedData'>AppendedData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/append_types/enum.AppendWrapper.html' title='routing::append_types::AppendWrapper'>AppendWrapper</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/priv_appendable_data/struct.PrivAppendedData.html' title='routing::priv_appendable_data::PrivAppendedData'>PrivAppendedData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/priv_appendable_data/struct.PrivAppendableData.html' title='routing::priv_appendable_data::PrivAppendableData'>PrivAppendableData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/pub_appendable_data/struct.PubAppendableData.html' title='routing::pub_appendable_data::PubAppendableData'>PubAppendableData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/structured_data/struct.StructuredData.html' title='routing::structured_data::StructuredData'>StructuredData</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/types/struct.MessageId.html' title='routing::types::MessageId'>MessageId</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/xor_name/struct.XorName.html' title='routing::xor_name::XorName'>XorName</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/messaging/mpid_header/struct.MpidHeader.html' title='routing::messaging::mpid_header::MpidHeader'>MpidHeader</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='struct' href='routing/messaging/mpid_message/struct.MpidMessage.html' title='routing::messaging::mpid_message::MpidMessage'>MpidMessage</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/messaging/mpid_message_wrapper/enum.MpidMessageWrapper.html' title='routing::messaging::mpid_message_wrapper::MpidMessageWrapper'>MpidMessageWrapper</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/client_errors/enum.GetError.html' title='routing::client_errors::GetError'>GetError</a>","impl <a class='trait' href='rustc_serialize/serialize/trait.Decodable.html' title='rustc_serialize::serialize::Decodable'>Decodable</a> for <a class='enum' href='routing/client_errors/enum.MutationError.html' title='routing::client_errors::MutationError'>MutationError</a>",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
