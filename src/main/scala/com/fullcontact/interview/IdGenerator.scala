package com.fullcontact.interview

object IdGenerator {
  /**
    * @param str input value
    * @return Converted str hashcode as a Long
    */
  def hash(str: String): Long = str.hashCode.toLong
}
