# example_usage.py
from main_library import Circle, Triangle, Rectangle, calculate_area, is_right_angled_triangle

def main():
    print("Демонстрация работы геометрической библиотеки")
    print("=" * 50)
    
    # Работа с кругом
    circle = Circle(5)
    print(f"Круг с радиусом 5:")
    print(f"  Площадь: {circle.area():.2f}")
    print(f"  Прямоугольный: {circle.is_right_angled()}")
    print()
    
    # Работа с треугольником
    triangle = Triangle(3, 4, 5)
    print(f"Треугольник со сторонами 3, 4, 5:")
    print(f"  Площадь: {triangle.area():.2f}")
    print(f"  Прямоугольный: {triangle.is_right_angled()}")
    print()
    
    # Полиморфное вычисление площади
    shapes = [Circle(3), Triangle(6, 8, 10), Rectangle(4, 7)]
    
    print("Полиморфное вычисление площадей:")
    for i, shape in enumerate(shapes, 1):
        area = calculate_area(shape)
        print(f"  Фигура {i}: {type(shape).__name__}, площадь: {area:.2f}")
    print()
    
    # Использование утилитарной функции
    print("Проверка треугольников через утилитарную функцию:")
    test_triangles = [(3, 4, 5), (5, 12, 13), (4, 5, 6)]
    for sides in test_triangles:
        is_right = is_right_angled_triangle(*sides)
        print(f"  Треугольник {sides}: прямоугольный = {is_right}")

if __name__ == "__main__":
    main()