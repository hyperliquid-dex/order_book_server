impl Oid {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying order ID value.
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
}