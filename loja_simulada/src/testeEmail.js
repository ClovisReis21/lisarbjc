var arr1 = ["a","b","c"];
var arr2 = ["a","c","d"];

if (arr1.length == arr2.length
    && arr1.every(function(u, i) {
        return u === arr2[i];
    })
) {
   console.log(true);
} else {
   console.log(false);
}