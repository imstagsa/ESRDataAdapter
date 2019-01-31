package net.digitaledge.data;

public class MapVariableValue {
	
	private String value;
	private String variable;
	private String description;
	
	public MapVariableValue(String variable)
	{
		this.variable = variable;
		this.value = new String();
		this.description = new String();
	}

	public MapVariableValue()
	{
		this.variable = new String();
		this.value = new String();
	}

	public MapVariableValue(String variable, String value)
	{
		this.variable = variable;
		this.value = value;
	}
	
	public MapVariableValue(String variable, String value, String description)
	{
		this.variable = variable;
		this.value = value;
		this.description = description;
	}
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public String getVariable() {
		return variable;
	}

	public void setVariable(String variable) {
		this.variable = variable;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
}