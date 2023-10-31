component  {

	variables.listener="";


	function before() {
		variables.listener="before;";
	}
	function invoke(string4) {
		variables.listener&=string4;
		return true;
	}
	function after() {
		variables.listener&="after;";
	}

	function getData() {
		return variables.listener;
	}
}