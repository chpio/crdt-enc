# Encrypted CRDTs

Main goal: make an encrypted layer that can be used to store CRDTs on
something like *syncthing*.

## tech

Changes are stored as CRDT-Ops or full-States. Resulting files are immutable (files are written only
once and never changed after that; but can be deleted) and content-addressable (the name of the file
is the hash of its content).

### compaction

Before writing out a change the device can decide to do a compaction on the data by writing out
the a full-state and removing all merged state and applied op files.

### header

* resembles luks (the actual encryption key used for the data isn't derived from the password(s))
  * no need to re-encrypt all files after key change (but can be re-encrypted if needed.
    this requires a compactation and the old encryption key needs to be stored to be able to apply ops
    coming from other devices that are still using the old key)
  * allows password management / multiple passwords
* store header as full-state CRDT
