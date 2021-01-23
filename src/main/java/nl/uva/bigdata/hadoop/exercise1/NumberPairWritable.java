package nl.uva.bigdata.hadoop.exercise1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class NumberPairWritable implements Writable {

  private int firstNumber;
  private int secondNumber;

  public NumberPairWritable() {}

  public NumberPairWritable(Integer firstNumber, Integer secondNumber) {
    this.firstNumber = firstNumber;
    this.secondNumber = secondNumber;
  }

  public void set(Integer firstNumber, Integer secondNumber) {
    this.firstNumber = firstNumber;
    this.secondNumber = secondNumber;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Implement me
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Implement me
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NumberPairWritable that = (NumberPairWritable) o;
    return firstNumber == that.firstNumber && secondNumber == that.secondNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hash(firstNumber, secondNumber);
  }
}