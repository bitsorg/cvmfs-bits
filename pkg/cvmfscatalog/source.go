// source.go — deleted. CatalogSource, HTTPSource, and LocalFSSource were only
// used by the Merge operation (merge.go) which has been removed. Catalog and
// object downloads are now handled directly in manifest.go (DownloadCatalog,
// DownloadObject) and dedup/catalog_seed.go.
package cvmfscatalog
