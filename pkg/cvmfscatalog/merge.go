// merge.go — deleted. Merge was superseded by BuildSubtree (subtree.go) which
// builds the lease subtree catalog locally without downloading the existing
// repository catalog, eliminating the O(CAS-count) bottleneck. The gateway's
// cvmfs_receiver grafts the subtree into the repository at commit time.
package cvmfscatalog
