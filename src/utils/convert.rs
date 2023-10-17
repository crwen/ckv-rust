pub fn u32vec_to_bytes(data: &[u32]) -> Vec<u8> {
    data.iter().flat_map(|val| val.to_be_bytes()).collect()
}

pub fn u64_to_bytes(data: u64) -> Vec<u8> {
    data.to_be_bytes().to_vec()
}

#[cfg(test)]
mod tests {

    use super::u32vec_to_bytes;

    #[test]
    fn convert_test() {
        // 0b_11 0000 0011 1001
        // 0b_101 1011 1010 0000
        // 0b_1000 0111 0000 0111
        let v = vec![12345_u32, 23456, 34567];
        let res = u32vec_to_bytes(&v);
        assert_eq!(res.len(), 12);

        let expected = [0, 0, 48, 57, 0, 0, 91, 160, 0, 0, 135, 7];
        assert_eq!(res, expected);
    }
}
