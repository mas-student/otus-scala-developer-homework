package ru.otus.jdbc.homework


import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import org.flywaydb.core.Flyway
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import ru.otus.jdbc.dao.slick.UserDaoSlickImpl
import ru.otus.jdbc.model.{Role, User}
import slick.jdbc.JdbcBackend.Database

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class UserDaoSlickImplTest extends AnyFreeSpec
  with ScalaCheckDrivenPropertyChecks with ScalaFutures  with ForAllTestContainer {
  override val container: PostgreSQLContainer = PostgreSQLContainer()

  var db: Database = _

  override def afterStart(): Unit = {
    super.afterStart()
    Flyway
      .configure()
      .dataSource(container.jdbcUrl, container.username, container.password)
      .load()
      .migrate()

    db = Database.forURL(container.jdbcUrl, container.username, container.password)
  }

  implicit val genRole: Gen[Role] = Gen.oneOf(Role.Admin, Role.Manager, Role.Reader)
  implicit val arbitraryRole: Arbitrary[Role] = Arbitrary(genRole)

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  implicit lazy val arbString: Arbitrary[String] = Arbitrary(arbitrary[List[Char]] map (_.filter(_ != 0).mkString))

  implicit val genUser: Gen[User] = for {
    id <- Gen.option(Gen.uuid)
    firstName <- arbitrary[String]
    lastName <- arbitrary[String]
    age <- arbitrary[Int]
    roles <- arbitrary[Seq[Role]]
  } yield User(id = id, firstName = firstName, lastName = lastName, age = age, roles = roles.toSet)

  implicit val arbitraryUser: Arbitrary[User] = Arbitrary(genUser)

    "getUser" - {
      "create and get unknown user" in {
        forAll { (users: Seq[User], userId: UUID) =>
          val dao = new UserDaoSlickImpl(db)
          users .foreach(dao.createUser(_).futureValue)

          dao.getUser(userId).futureValue shouldBe None
        }
      }
    }


    "updateUser" - {
      "update known user - keep other users the same" in {
        forAll { (users: Seq[User], user1: User, user2: User) =>
          val dao = new UserDaoSlickImpl(db)
          val createdUsers = users.map(dao.createUser(_).futureValue)
          val createdUser = dao.createUser(user1).futureValue
          val toUpdate = user2.copy(id = createdUser.id)

          dao.updateUser(toUpdate)

          dao.getUser(toUpdate.id.get).futureValue shouldBe Some(toUpdate)
          createdUsers.foreach { u => dao.getUser(u.id.get).futureValue shouldBe Some(u)
          }
        }
      }
    }

    "delete known user - keep other users the same" in {
      forAll { (users1: Seq[User], user1: User) =>
        val dao = new UserDaoSlickImpl(db)
        val createdUsers1 = users1.map(dao.createUser(_).futureValue)
        val createdUser = dao.createUser(user1).futureValue

        dao.getUser(createdUser.id.get).futureValue shouldBe Some(createdUser)
        dao.deleteUser(createdUser.id.get).futureValue shouldBe Some(createdUser)
        dao.getUser(createdUser.id.get).futureValue shouldBe None

        createdUsers1.foreach { u => dao.getUser(u.id.get).futureValue shouldBe Some(u)}
      }
    }


        "findByLastName" in {
          forAll { (users1: Seq[User], lastName: String, users2: Seq[User]) =>
            val dao               = new UserDaoSlickImpl(db)
            val withOtherLastName = users1.filterNot(_.lastName == lastName)
            val withLastName      = users2.map(_.copy(lastName = lastName))

            dao.deleteAll().futureValue

            withOtherLastName.foreach(dao.createUser(_).futureValue)
            val createdWithLasName = withLastName.map(dao.createUser(_).futureValue)

            dao.findByLastName(lastName).futureValue.toSet shouldBe createdWithLasName.toSet
          }
        }

    "findAll" in {
      forAll { users: Seq[User] =>
        val dao = new UserDaoSlickImpl(db)

        dao.deleteAll().futureValue

        val createdUsers = users.map(dao.createUser(_).futureValue)

        dao.findAll().futureValue.toSet shouldBe createdUsers.toSet
      }
    }



  override def beforeStop(): Unit = {
    db.close()
    super.beforeStop()
  }

}
