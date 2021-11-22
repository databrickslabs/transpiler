package spl.ast

import fastparse._

sealed trait OperatorSymbol {
  def P[_: P]: P[OperatorSymbol] = symbols.map(_ => this).opaque(getClass.getSimpleName)
  def symbols[_: P]: P[Unit] = toString
  def precedence: Int
}

sealed trait Straight extends OperatorSymbol
sealed trait Relational extends Straight

case object Or extends Straight {
  override def toString: String = "OR"
  override val precedence: Int = 9
}

case object And extends Straight {
  override def symbols[_: P]: P[Unit] = IgnoreCase("AND") | " "
  override def toString: String = "AND"
  override val precedence: Int = 8
}

case object LessThan extends Relational {
  override def toString: String = "<"
  override val precedence: Int = 7
}

case object GreaterThan extends Relational {
  override def toString: String = ">"
  override val precedence: Int = 7
}

case object GreaterEquals extends Relational {
  override def toString: String = ">="
  override val precedence: Int = 7
}

case object LessEquals extends Relational {
  override def toString: String = "<="
  override val precedence: Int = 7
}

case object Equals extends Relational {
  override def symbols[_: P]: P[Unit] = "=" | "::"
  override def toString: String = "="
  override val precedence: Int = 7
}

case object InList extends Straight {
  override def symbols[_: P]: P[Unit] = " " ~~ IgnoreCase(toString)
  override def toString: String = "IN"
  override val precedence: Int = 7
}

case object NotEquals extends Relational {
  override def toString: String = "!="
  override val precedence: Int = 7
}

case object Concatenate extends Straight {
  override def toString: String = "."
  override val precedence: Int = 5
}

case object Add extends Straight {
  override def toString: String = "+"
  override val precedence: Int = 4
}

case object Subtract extends Straight {
  override def toString: String = "-"
  override val precedence: Int = 4
}

case object Multiply extends Straight {
  override def toString: String = "*"
  override val precedence: Int = 3
}

case object Divide extends Straight {
  override def toString: String = "/"
  override val precedence: Int = 3
}

case object UnaryNot extends OperatorSymbol {
  override def precedence: Int = 2
  override def toString: String = "NOT"
}