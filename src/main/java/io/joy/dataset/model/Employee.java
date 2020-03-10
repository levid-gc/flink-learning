package io.joy.dataset.model;

/**
 * @author Owen
 */
public class Employee {
  
  private String userName;
  private Integer officeNumber;

  public Employee() {
  }

  public Employee(String userName, Integer officeNumber) {
    this.userName = userName;
    this.officeNumber = officeNumber;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public Integer getOfficeNumber() {
    return officeNumber;
  }

  public void setOfficeNumber(Integer officeNumber) {
    this.officeNumber = officeNumber;
  }

  @Override
  public String toString() {
    return "Employee [userName=" + userName + ", officeNumber=" + officeNumber + "]";
  }
}