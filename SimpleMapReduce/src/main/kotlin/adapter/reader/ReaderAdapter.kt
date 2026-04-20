package adapter.reader

import model.CityModel
import repo.SQLRepo
import usecase.io.DataReader

class ReaderAdapter(
    repo: SQLRepo,
) : DataReader<CityModel> {
    override fun read(): Iterable<CityModel> {
        TODO("Not yet implemented")
    }
}
