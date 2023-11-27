package it.jobtech.graphenj.repositories

import it.jobtech.graphenj.configuration.model.bookmark.JtBookmarkStorageDetail.Database
import it.jobtech.graphenj.configuration.model.bookmark.SqlLiteDbType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }

import java.io.File

class JtBookmarkRepositoryDBTest extends AnyFunSuite with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private val storageDetail = Database("jdbc:sqlite:src/test", "", "", "test.db", "test_bookmark", SqlLiteDbType)

  override def afterEach(): Unit = {
    val file = new File("src/test/test.db")
    if (file.exists()) file.delete()
  }

  test("JtBookmarkRepositoryJDBC correctly read, write and delete bookmarks") {
    val repo          = new JtBookmarkRepositoryJDBC(storageDetail)
    val bookmarkEntry = "testBookmark"
    val value         = "testValue"
    repo.writeBookmark(bookmarkEntry, value) shouldBe Right(())
    val res           = repo.getLastBookmark[String](bookmarkEntry)

    res.isRight shouldBe true
    val maybeBookmarkRes = res.right.get

    maybeBookmarkRes.isDefined shouldBe true
    val bookmarkRes = maybeBookmarkRes.get

    bookmarkRes.entry shouldBe bookmarkEntry
    bookmarkRes.lastRead shouldBe value
    repo.deleteLastBookmark(bookmarkEntry) shouldBe Right(())
    repo.getLastBookmark[String](bookmarkEntry) shouldBe Right(None)
  }

  test("JtBookmarkRepositoryJDBC correctly read bookmarks without an existent table") {
    val repo          = new JtBookmarkRepositoryJDBC(storageDetail)
    val bookmarkEntry = "testBookmark"

    repo.getLastBookmark[String](bookmarkEntry) shouldBe Right(None)
  }

}
