package main.sstable;

class BlockInfo {
    long offset;
    long length;

    BlockInfo(long offset, long length) {
        this.offset = offset;
        this.length = length;
    }
}
