public class Address {
    private final int value;

    public Address(int value) {
        if (value < 0) throw new IllegalArgumentException("Address cannot be negative");
        this.value = value;
    }

    public int getValue() { return value; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Address address = (Address) o;
        return value == address.value;
    }

    @Override
    public int hashCode() { return Integer.hashCode(value); }

    @Override
    public String toString() { return String.valueOf(value); }
}