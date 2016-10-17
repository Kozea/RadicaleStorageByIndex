# RadicaleStorageByIndex

This is a storage extension for [Radicale](/Kozea/Radicale) that optimizes reporting using a sqlite index.

## Configuration

You have to change the storage in your `config` file and set which fields should be indexed:

```ini
...

[storage]
type = radicale_storage_by_index
radicale_storage_by_index_fields = dtstart, dtend, uid, summary

```

## Usage

Use Radicale like you normally would but faster.


## License

Copyright Florian Mounier Kozea 2016 according to BSD license terms.
