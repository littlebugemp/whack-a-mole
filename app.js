const square = document.querySelectorAll('.square')
const mole = document.querySelector('.mole')
const timeleft = document.querySelector('#time-left')
let score = document.querySelector("#score")

let result = 0
function randomSquare(){
    square.forEach(className => {
        className.classList.remove('mole')
    })

    let randomPosition = square[Math.floor(Math.random() * 9)]
    console.log("Random Positon --", randomPosition)
    randomPosition.classList.add('mole')


    //assign the id of thr randompositon to hitPosition for us to use later
    hitPosition = randomPosition.id
}

square.forEach(id => {
    id.addEventListener('mouseup', () => {
        if(id.id === hitPosition){
            result = result + 1
            score.textContent = result
        }
    })
})

function moveMole(){
    let timerId = null
    timerId = setInterval(randomSquare,500)
}

moveMole()