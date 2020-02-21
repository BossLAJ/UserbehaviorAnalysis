val array =(1 to 10).toArray
var index = 0
while (index < array.length-1){
	var temp = array(index)
	array(index) = array(index+1 )
	array(index+1) = temp
	index += 2
}
println(array.mkString(","))