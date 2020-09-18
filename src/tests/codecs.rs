use crate::errors::*;
use crate::codecs::*;
use crate::nibble::Nibble;

#[test]
fn test_byte_rle_decoder() {
    let encoded_test: ByteRLEDecoder = hex!("FC DEAD BEEF 03 00 FC CAFE BABE")[..].into();
    let dec: OrcResult<Vec<u8>> = encoded_test.collect();
    assert_eq!(dec.unwrap(), hex!("DEAD BEEF 00 00 00 00 00 00 CAFE BABE"));
}

#[test]
fn test_bool_rle_decoder() {
    let encoded_test : BooleanRLEDecoder = hex!("FF 80")[..].into();
    let dec: OrcResult<Vec<bool>> = encoded_test.collect();
    assert_eq!(dec.unwrap(), &[true, false, false, false, false, false, false, false]);
}

#[test]
fn test_varint128_decoder() {
    let encoded_test : VarDecoder<u128> = hex!("
        96 01    96 01    00
        FF FF FF FF FF FF FF FF FF FF FF FF FF FF FF FF FF FF 03")[..].into();
    let dec: OrcResult<Vec<u128>> = encoded_test.collect();
    assert_eq!(dec.unwrap(), &[150, 150, 0, 340282366920938463463374607431768211455]);
}

#[test]
fn test_signed_varint128_decoder() {
    let encoded_test : VarDecoder<i128> = hex!("
        96 01    96 01    00 01 02 03 04")[..].into();
    let dec: OrcResult<Vec<i128>> = encoded_test.collect();
    assert_eq!(dec.unwrap(), &[75, 75, 0, -1, 1, -2, 2]);
}

#[test]
fn test_intrle1_decoder() {
    let encoded_test : RLE1<u128> = hex!("03 00 07     FB 02 03 04 07 0B")[..].into();
    let dec: OrcResult<Vec<u128>> = encoded_test.collect();
    assert_eq!(dec.unwrap(), &[7,7,7,7,7,7,2,3,4,7,11]);

    let encoded_test : RLE1<i128> = hex!("03 00 07     FB 02 03 04 07 0B")[..].into();
    let dec: OrcResult<Vec<i128>> = encoded_test.collect();
    assert_eq!(dec.unwrap(), &[-4,-4,-4,-4,-4,-4,1,-2,2,-4,-6]);
}

#[test]
fn test_intrle2_decoder() {
    // First a short repeat.
    // Then direct
    // Then a patched base
    // and last a delta.
    // All of these examples from from the specification.
    #[rustfmt::skip]
    let encoded_test : RLE2<u128> = hex!("
        0a 27 10
        5e 03 5c a1 ab 1e dead beef
        8e 13 2b 21 07 d0 1e 00 14 70 28 32 3c 46 50 5a 64 6e 78 82 8c 96 a0 aa b4 be fc e8
        c6 09 02 02 22 42 42 46
    ")[..].into();
    let dec: OrcResult<Vec<u128>> = encoded_test.collect();
    #[rustfmt::skip]
    assert_eq!(dec.unwrap(), vec![
        10000, 10000, 10000, 10000, 10000,
        23713, 43806, 57005, 48879,
        2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190,
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29,
    ]);
}

#[test]
fn test_nibble() {
    let mut nib = Nibble{
        buf: &hex!("01 23 45 67 89 AB CD EF")[..],
        start: 0
    };
    assert_eq!(nib.read::<u64>(4, "").unwrap(), 0x0);
    assert_eq!(nib.read::<u64>(8, "").unwrap(), 0x12);
    assert_eq!(nib.read::<u64>(12, "").unwrap(), 0x345);
    assert_eq!(nib.read::<u64>(1, "").unwrap(), 0);
    assert_eq!(nib.read::<u64>(7, "").unwrap(), 0x67);
    assert_eq!(nib.read::<u64>(5, "").unwrap(), 0x11);
}