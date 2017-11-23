package networking;

/**
 * Struct to represent a Machine. Contains IP Address and Port information.
 */
public class Machine {

    public final String ipAddress;
    public final int port;

    public Machine(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Machine machine = (Machine) o;

        if (port != machine.port) return false;
        return ipAddress != null ? ipAddress.equals(machine.ipAddress) : machine.ipAddress == null;

    }

    @Override
    public int hashCode() {
        int result = ipAddress != null ? ipAddress.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "Machine{" +
                "ipAddress='" + ipAddress + '\'' +
                ", port=" + port +
                '}';
    }

}
