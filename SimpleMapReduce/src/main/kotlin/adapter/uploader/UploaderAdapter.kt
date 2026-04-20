package adapter.uploader

import repo.MinIORepo
import usecase.io.DataUploader

class UploaderAdapter(
    repo: MinIORepo,
) : DataUploader {
    override fun upload(key: String) {
        TODO("Not yet implemented")
    }
}
