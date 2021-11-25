package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Add, And, Cast, ElementAt, Expression, ExpressionDescription, GreaterThanOrEqual, LessThanOrEqual, Literal, Multiply, Pow, RuntimeReplaceable, StringSplit, SubstringIndex, Subtract}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType}

@ExpressionDescription(
  usage = "_FUNC_(cidr, ip) - Matches IP address string with the supplied CIDR string",
  since = "3.3.0")
case class CidrMatch(cidr: Expression, ip: Expression) extends RuntimeReplaceable {

  override def child: Expression = And(GreaterThanOrEqual(ipAddr, lowAddr), LessThanOrEqual(ipAddr, highAddr))
  override def exprsReplaced: Seq[Expression] = Seq(cidr, ip)
  override def flatArguments: Iterator[Any] = Iterator(cidr, ip)

  // TODO: add special handling for /8, /16, and /24 with StartsWith()
  private def cidrMatch = And(
    GreaterThanOrEqual(ipAddr, lowAddr),
    LessThanOrEqual(ipAddr, highAddr))

  private def ipAddr: Add = aton(ip)
  private def lowAddr: Add = aton(SubstringIndex(cidr, Literal.create("/"), Literal.create(1)))
  private def highAddr: Add = Add(lowAddr, numAddr)

  private def numAddr = Subtract(
    Cast(Pow(
      Literal.create(2.0),
      Subtract(
        Literal.create(32.0),
        Cast(SubstringIndex(
          cidr,
          Literal.create("/"),
          Literal.create(-1)
        ), DoubleType)
      )
    ), IntegerType),
    Literal.create(1))

  private def aton(addr: Expression): Add = {
    val bytes = new StringSplit(addr, Literal.create("\\."))
    Add(Add(Add(
      addrMult(bytes, 1, 256*256*256),
      addrMult(bytes, 2, 256*256)),
      addrMult(bytes, 3, 256)),
      addrMult(bytes, 4, 1))
  }

  private def addrMult(bytes: Expression, offset: Int, multiple: Int) =
      Multiply(
        Cast(ElementAt(bytes, Literal.create(offset)), IntegerType),
        Literal.create(multiple))
}

