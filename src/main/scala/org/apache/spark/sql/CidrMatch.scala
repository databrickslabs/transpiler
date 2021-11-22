package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Add, And, Cast, ElementAt, Expression, ExpressionDescription, GreaterThanOrEqual, LessThanOrEqual, Literal, Multiply, Pow, RuntimeReplaceable, StringSplit, SubstringIndex, Subtract}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}

@ExpressionDescription(
  usage = "_FUNC_(cidr, ip) - Matches IP address string with the supplied CIDR string",
  since = "3.3.0")
case class CidrMatch(cidr: Expression, ip: Expression) extends RuntimeReplaceable {
  override def nullable: Boolean = false
  override def dataType: DataType = BooleanType

  override def exprsReplaced: Seq[Expression] = Seq(cidrMatch)
  override def child: Expression = cidr // TODO: is this correct?..

  // TODO: add special handling for /8, /16, and /24 with StartsWith()
  private def cidrMatch = And(
    GreaterThanOrEqual(ipAddress, lowAddress),
    LessThanOrEqual(ipAddress, highAddress))

  private def ipAddress: Add = aton(ip)
  private def lowAddress: Add = aton(SubstringIndex(cidr, Literal.create("/"), Literal.create(1)))
  private def highAddress: Add = Add(lowAddress, numAddresses)

  private def numAddresses = Subtract(
    Pow(
      Literal.create(2),
      Subtract(
        Literal.create(32),
        SubstringIndex(
          cidr,
          Literal.create("/"),
          Literal.create(-1)
        )
      )
    ), Literal.create(1))

  private def aton(addr: Expression): Add = {
    val bytes = new StringSplit(addr, Literal.create("\\."))
    Add(Add(Add(
      addrMult(bytes, 0, 256*256*256),
      addrMult(bytes, 1, 256*256)),
      addrMult(bytes, 2, 256)),
      addrMult(bytes, 3, 1))
  }

  private def addrMult(bytes: Expression, offset: Int, multiple: Int) =
    Cast(
      Multiply(
        ElementAt(bytes, Literal.create(offset)),
        Literal.create(multiple)),
      IntegerType)
}