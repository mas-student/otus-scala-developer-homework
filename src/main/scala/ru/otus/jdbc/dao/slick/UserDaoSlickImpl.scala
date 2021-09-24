package ru.otus.jdbc.dao.slick



import ru.otus.jdbc.model.{Role, User}
import slick.dbio.Effect
import slick.jdbc.PostgresProfile
import slick.jdbc.PostgresProfile.api._
import slick.sql.{FixedSqlAction, FixedSqlStreamingAction}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}




class UserDaoSlickImpl(db: Database)(implicit ec: ExecutionContext) {
  import UserDaoSlickImpl._

  def getUser(userId: UUID): Future[Option[User]] = {
    val res = for {
      user  <- users.filter(user => user.id === userId).result.headOption
      roles <- usersToRoles.filter(_.usersId === userId).map(_.rolesCode).result.map(_.toSet)
    } yield user.map(_.toUser(roles))

    db.run(res)
  }

  def createUser(user: User): Future[User] = {
    for {
      userId: UUID <- db.run((users returning users.map(_.id) += UserRow.fromUser(user)))
      _ <- db.run(DBIO.seq(usersToRoles ++= user.roles.map(role => (userId, role))))
      user <- Future { User(Some(userId), user.firstName, user.lastName, user.age, user.roles) }
    } yield user

  }

  def updateUser(user: User): Future[Unit] = {
    user.id match {
      case Some(userId) =>
        val updateUser = users
          .filter(_.id === userId)
          .map(u => (u.firstName, u.lastName, u.age))
          .update((user.firstName, user.lastName, user.age))
        val insertUser = users += UserRow.fromUser(user)

        val deleteRoles = usersToRoles.filter(_.usersId === userId).delete
        val insertRoles = usersToRoles ++= user.roles.map(userId -> _)

        val updateAction = updateUser >> deleteRoles >> insertRoles >> DBIO.successful(())
        val insertAction = insertUser >> insertRoles >> DBIO.successful(())

        for {
          userRowOption <- db.run(users.filter(user => user.id === userId).result.headOption)

          res <- if (userRowOption.isEmpty)
            db.run(insertAction.transactionally)
            else db.run(updateAction.transactionally)

        } yield res

      case None => Future.successful(())
    }

  }

  def deleteUser(userId: UUID): Future[Option[User]] = {
    for {
      usersToRolesRows: Seq[(UUID, Role)] <- db.run(usersToRoles.result)
      userIdToRoles: Map[UUID, Set[Role]] = usersToRolesRows.groupBy(v => v._1).map{case (k, v) => (k, v.map(vv => vv._2).toSet)}

      userRowOption <- db.run(users.filter(user => user.id === userId).result.headOption)
      userOption = userRowOption.map(userRow => userRow.toUser(userRow.id.flatMap(userIdToRoles.get).fold(Set[Role]())(v => v)))

      _ <- userRowOption match {
        case Some(_) => db.run(usersToRoles.filter(_.usersId === userId).delete)
        case None => Future{0}
      }
      _ <- userRowOption match {
        case Some(_) => db.run(users.filter(_.id === userId).delete)
        case None => Future{0}
      }

    } yield userOption

  }

  private def findByCondition(condition: Users => Rep[Boolean]): Future[Vector[User]] = {
    for {
      usersToRolesRows: Seq[(UUID, Role)] <- db.run(usersToRoles.result)
      userIdToRoles: Map[UUID, Set[Role]] = usersToRolesRows.groupBy(v => v._1).map{case (k, v) => (k, v.map(vv => vv._2).toSet)}

      userRows <- db.run(users.filter(condition).result)

      users = userRows.map(userRow => userRow.toUser(userRow.id.flatMap(userIdToRoles.get).fold(Set[Role]())(v => v)))

    } yield users.toVector

  }

  def findByLastName(lastName: String): Future[Seq[User]] = {

    for {
      usersToRolesRows: Seq[(UUID, Role)] <- db.run(usersToRoles.result)
      userIdToRoles: Map[UUID, Set[Role]] = usersToRolesRows.groupBy(v => v._1).map{case (k, v) => (k, v.map(vv => vv._2).toSet)}

      userRows <- db.run(users.filter(user => user.lastName === lastName).result)

      users = userRows.map(userRow => userRow.toUser(userRow.id.flatMap(userIdToRoles.get).fold(Set[Role]())(v => v)))

    } yield users

  }

  def findAll(): Future[Seq[User]] = {
    for {
      userRows: Seq[UserRow] <- db.run(users.result)
      r0: Seq[(UUID, Role)] <- db.run(usersToRoles.result)
      r1: Map[UUID, Set[Role]] = r0.groupBy(v => v._1).map{case (k, v) => (k, v.map(vv => vv._2).toSet)}

      r: Seq[User] = userRows.map(
        userRow => userRow.id match {
          case Some(uuid) => r1.get(uuid) match {
            case Some(roles) => userRow.toUser(roles)
            case None => userRow.toUser(Set())
          }
          case None => userRow.toUser(Set())
        }
      )

    } yield r
  }

  def deleteAll(): Future[Unit] = for {
    _ <- db.run(usersToRoles.delete)
    _ <- db.run(users.delete)
  } yield ()
}

object UserDaoSlickImpl {
  implicit val rolesType: BaseColumnType[Role] = MappedColumnType.base[Role, String](
    {
      case Role.Reader => "reader"
      case Role.Manager => "manager"
      case Role.Admin => "admin"
    },
    {
        case "reader"  => Role.Reader
        case "manager" => Role.Manager
        case "admin"   => Role.Admin
    }
  )


  case class UserRow(
      id: Option[UUID],
      firstName: String,
      lastName: String,
      age: Int
  ) {
    def toUser(roles: Set[Role]): User = User(id, firstName, lastName, age, roles)
  }

  object UserRow extends ((Option[UUID], String, String, Int) => UserRow) {
    def fromUser(user: User): UserRow = UserRow(user.id, user.firstName, user.lastName, user.age)
  }

  class Users(tag: Tag) extends Table[UserRow](tag, "users") {
    val id        = column[UUID]("id", O.PrimaryKey, O.AutoInc)
    val firstName = column[String]("first_name")
    val lastName  = column[String]("last_name")
    val age       = column[Int]("age")

    val * = (id.?, firstName, lastName, age).mapTo[UserRow]
  }

  val users: TableQuery[Users] = TableQuery[Users]

  class UsersToRoles(tag: Tag) extends Table[(UUID, Role)](tag, "users_to_roles") {
    val usersId   = column[UUID]("users_id")
    val rolesCode = column[Role]("roles_code")

    val * = (usersId, rolesCode)
  }

  val usersToRoles = TableQuery[UsersToRoles]
}
