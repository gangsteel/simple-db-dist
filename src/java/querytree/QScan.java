package querytree;

class QScan implements QueryTree {
    
    private final String tableName;
    
    QScan(String name) {
        tableName = name;
    }
    
    @Override
    public String toString() {
        return "SCAN(" + tableName + ")"; //TODO: Confirm with the grammar
    }
    
    @Override
    public boolean equals(Object obj) {
        throw new RuntimeException("not implemented");
    }
    
    @Override
    public int hashCode() {
        throw new RuntimeException("not implemented");
    }
}
