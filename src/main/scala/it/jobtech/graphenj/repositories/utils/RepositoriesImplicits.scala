package it.jobtech.graphenj.repositories.utils

import java.sql.ResultSet

object RepositoriesImplicits {

  implicit class ResultSetStream(resultSet: ResultSet) {

    def toStream: Stream[ResultSet] = {
      new Iterator[ResultSet] {
        def hasNext: Boolean  = resultSet.next()
        def next(): ResultSet = resultSet
      }.toStream
    }
  }

}
