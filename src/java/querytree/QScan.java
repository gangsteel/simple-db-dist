package querytree;

class QScan implements QueryTree {
    
    private final int tableid;
    
    QScan(int tableid) {
        this.tableid = tableid;
    }
    
    @Override
    public String toString() {
        return "SCAN(" + tableid + ")"; //TODO: Confirm with the grammar
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
