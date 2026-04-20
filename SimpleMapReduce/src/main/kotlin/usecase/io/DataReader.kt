package usecase.io

interface DataReader<R> {
    fun read(): Iterable<R>
}
